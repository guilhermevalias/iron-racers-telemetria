#!/usr/bin/env python3
r"""
Telemetria de box - Fórmula SAE | FuelTech FT500 (FTCAN 2.0)
============================================================

  CAN da FT500 --ESP-NOW--> ESP32 RX --USB/UART 115200--> [ESTE SCRIPT] --> InfluxDB

Cada linha da serial segue o formato (terminada em \n):
    ID,B0,B1,B2,B3,B4,B5,B6,B7        ex.: 14000000,19,136,128,7,208,0,0,0
(ID em hexadecimal; B0..B7 em decimal, 0-255.)

CAMADAS DE PROTEÇÃO CONTRA RUÍDO
  1. Bytes      -> buffer próprio (linhas parciais), decodificação ASCII estrita e
                   limite de tamanho de linha.
  2. Formato    -> regex rígida: exatamente 9 campos, ID hexa, bytes decimais 0-255.
  3. ID         -> frames com ID desconhecido são ignorados.
  4. Física     -> "sanity check" por variável (faixas seguras do motor).
  5. Transporte -> reconexão automática da serial + gravação em lote (assíncrona)
                   no InfluxDB.

DEPENDÊNCIAS
    pip install pyserial influxdb-client

VARIÁVEIS DE AMBIENTE
    INFLUX_TOKEN   (obrigatória no modo real)
    INFLUX_URL     (padrão: http://localhost:8086)
    INFLUX_ORG     (padrão: fsae)
    INFLUX_BUCKET  (padrão: telemetria)
    PORTA_SERIAL   (opcional, ex.: COM5 ou /dev/ttyUSB0; vazio = autodetecta a ESP32)
    LOG_LEVEL      (padrão: INFO; use DEBUG para ver cada descarte)

Se nenhuma porta serial for encontrada ao iniciar, o script executa uma SIMULAÇÃO
com pacotes aleatórios (mock data) para testar o parser. Nada é gravado no InfluxDB.
"""

from __future__ import annotations

import logging
import math
import os
import random
import re
import sys
import time
from collections import Counter
from typing import Callable, Optional, Sequence

import serial
import serial.tools.list_ports
from influxdb_client import InfluxDBClient, Point, WriteOptions, WritePrecision

log = logging.getLogger("telemetria")

# ============================================================================
# CONFIGURAÇÃO
# ============================================================================
PORTA_SERIAL = os.getenv("PORTA_SERIAL") or None  # None -> autodetecta
BAUDRATE = 115200
TIMEOUT_SERIAL_S = 1.0        # ser.read() nunca bloqueia para sempre (permite Ctrl+C e relatório)
INTERVALO_RECONEXAO_S = 2.0
INTERVALO_RELATORIO_S = 10.0
TAMANHO_MAX_LINHA = 128       # uma linha válida tem no máximo ~42 bytes

INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "")
INFLUX_ORG = os.getenv("INFLUX_ORG", "fsae")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "telemetria")
INFLUX_MEASUREMENT = "motor_ft500"

# VID USB das pontes serial comuns em placas ESP32: CP210x, CH340/CH9102, FTDI e USB nativa (S3/C3)
VIDS_PONTES_USB = {0x10C4, 0x1A86, 0x0403, 0x303A}

# IDs CAN do protocolo FTCAN 2.0 usados neste projeto
ID_RPM_TPS_MAP = 0x14000000
ID_TEMPERATURAS_LAMBDA = 0x14000001
ID_BATERIA = 0x14000002

# Faixas fisicamente possíveis (mínimo, máximo), inclusivas.
LIMITES_SANIDADE: dict[str, tuple[float, float]] = {
    "rpm":          (0.0, 12000.0),
    "tps_pct":      (0.0, 100.0),
    "map_bar":      (0.0, 3.0),
    "temp_motor_c": (0.0, 150.0),
    "temp_ar_c":    (0.0, 100.0),
    "lambda":       (0.5, 1.5),
    "vbat_v":       (8.0, 16.0),
}

# RUÍDO (camada 2): formato exato "ID,B0,...,B7". Rejeita de cara linhas truncadas,
# campos faltando/sobrando, sinais, espaços, '_', letras fora do ID e qualquer caractere
# não-ASCII (ex.: o log de boot da ESP32 lido na velocidade errada).
_REGEX_LINHA = re.compile(r"[0-9A-Fa-f]{1,8}(?:,[0-9]{1,3}){8}")


# ============================================================================
# 1) PARSER MATEMÁTICO (puro: sem I/O, sem validação de faixa)
# ============================================================================
def _u16_be(msb: int, lsb: int) -> int:
    """Une 2 bytes em um inteiro de 16 bits big-endian (byte mais significativo primeiro)."""
    return (msb * 256) + lsb


def _decodificar_rpm_tps_map(b: Sequence[int]) -> dict[str, float]:
    return {
        "rpm": float(_u16_be(b[0], b[1])),
        "tps_pct": b[2] / 2.55,
        "map_bar": (_u16_be(b[3], b[4]) - 1000) / 1000,
    }


def _decodificar_temperaturas_lambda(b: Sequence[int]) -> dict[str, float]:
    return {
        "temp_motor_c": _u16_be(b[0], b[1]) / 10,
        "temp_ar_c": _u16_be(b[2], b[3]) / 10,
        "lambda": (b[4] / 100) + 0.5,
    }


def _decodificar_bateria(b: Sequence[int]) -> dict[str, float]:
    return {"vbat_v": b[0] / 10}


_DECODIFICADORES: dict[int, Callable[[Sequence[int]], dict[str, float]]] = {
    ID_RPM_TPS_MAP: _decodificar_rpm_tps_map,
    ID_TEMPERATURAS_LAMBDA: _decodificar_temperaturas_lambda,
    ID_BATERIA: _decodificar_bateria,
}


def decodificar_frame(can_id: int, payload: Sequence[int]) -> Optional[dict[str, float]]:
    """Converte os 8 bytes de um frame CAN em variáveis de engenharia (FTCAN 2.0, big-endian).

    Função matemática pura. Devolve None para ID desconhecido (RUÍDO camada 3).
    Todos os valores saem como float: o InfluxDB fixa o tipo do campo no primeiro
    ponto gravado, e misturar int com float geraria "field type conflict".
    """
    decodificador = _DECODIFICADORES.get(can_id)
    if decodificador is None or len(payload) != 8:
        return None
    return decodificador(payload)


# ============================================================================
# 2) FILTROS DE RUÍDO
# ============================================================================
def interpretar_linha(linha: str) -> Optional[tuple[int, list[int]]]:
    """RUÍDO (camada 2): valida o formato da linha e devolve (id_can, [B0..B7]) ou None."""
    linha = linha.strip()  # tira '\r', '\n' e espaços nas pontas
    if not _REGEX_LINHA.fullmatch(linha):
        return None
    campos = linha.split(",")
    payload = [int(c) for c in campos[1:]]
    if max(payload) > 255:  # a regex aceita até 999; um byte CAN só vai até 255
        return None
    return int(campos[0], 16), payload


def validar_sanidade(valores: dict[str, float]) -> dict[str, float]:
    """RUÍDO (camada 4): mantém só as variáveis dentro da física do motor.

    A checagem é por variável: um sensor fora da faixa não invalida os demais do
    mesmo frame. NaN também é rejeitado (qualquer comparação com NaN dá False).
    Limitação: valor corrompido que continue dentro da faixa não é detectável;
    para isso seria preciso um checksum/CRC na linha enviada pela ESP32.
    """
    validos: dict[str, float] = {}
    for nome, valor in valores.items():
        minimo, maximo = LIMITES_SANIDADE[nome]
        if minimo <= valor <= maximo:
            validos[nome] = valor
        else:
            log.debug("Sanity check: %s=%.3f fora de [%s, %s] -> descartado", nome, valor, minimo, maximo)
    return validos


def processar_linha(linha: str, stats: Counter) -> Optional[tuple[int, dict[str, float]]]:
    """Pipeline completo, sem I/O: texto -> formato -> parser -> sanity check.

    Devolve (id_can, valores_validos) ou None se não sobrou nada aproveitável.
    """
    if not linha.strip():
        return None  # linha em branco não é erro
    stats["linhas_lidas"] += 1

    frame = interpretar_linha(linha)
    if frame is None:
        stats["descartes_formato"] += 1
        log.debug("Linha corrompida descartada: %r", linha)
        return None

    can_id, payload = frame
    valores = decodificar_frame(can_id, payload)
    if valores is None:
        stats["descartes_id_desconhecido"] += 1
        return None

    validos = validar_sanidade(valores)
    stats["campos_fora_da_faixa"] += len(valores) - len(validos)
    if not validos:
        stats["frames_totalmente_rejeitados"] += 1
        return None

    stats["frames_validos"] += 1
    return can_id, validos


# ============================================================================
# 3) GRAVAÇÃO NO INFLUXDB
# ============================================================================
class GravadorInflux:
    """Grava os pontos em lote, em thread própria, sem nunca travar a leitura da serial."""

    def __init__(self, url: str, token: str, org: str, bucket: str) -> None:
        self._org = org
        self._bucket = bucket
        self._client = InfluxDBClient(url=url, token=token, org=org, timeout=5_000)
        if not self._client.ping():
            log.warning("InfluxDB não respondeu em %s. Os pontos ficam em fila e serão reenviados.", url)

        # Escrita em LOTE: write() só enfileira (microssegundos) e uma thread interna envia
        # o lote a cada 250 ms ou 100 pontos. Se a rede/banco engasgar, a serial continua
        # sendo lida: o buffer da UART do sistema operacional é pequeno (~4 KB, ou seja,
        # ~350 ms a 115200 bps) e bloquear aqui faria perder bytes.
        self._write_api = self._client.write_api(
            write_options=WriteOptions(
                batch_size=100,
                flush_interval=250,
                jitter_interval=0,
                retry_interval=2_000,
                max_retries=3,
                max_retry_delay=10_000,
                exponential_base=2,
                max_close_wait=10_000,  # no Ctrl+C, espera no máx. 10 s para esvaziar a fila
            ),
            error_callback=self._ao_falhar,
            retry_callback=self._ao_retentar,
        )

        # Relógio: ver _timestamp_ns()
        self._base_wall_ns = time.time_ns()
        self._base_perf_ns = time.perf_counter_ns()
        self._ultimo_ts_ns = 0

    def gravar(self, can_id: int, valores: dict[str, float]) -> None:
        ponto = (
            Point(INFLUX_MEASUREMENT)
            .tag("ecu", "FT500")
            .time(self._timestamp_ns(), WritePrecision.NS)
        )
        for nome, valor in valores.items():
            ponto.field(nome, float(valor))  # sempre float (evita conflito de tipo no Influx)
        self._write_api.write(
            bucket=self._bucket, org=self._org, record=ponto, write_precision=WritePrecision.NS
        )

    def fechar(self) -> None:
        """Descarrega o lote pendente e fecha a conexão."""
        try:
            self._write_api.close()
        finally:
            self._client.close()

    def _timestamp_ns(self) -> int:
        """Instante de recepção no PC (UTC, ns), com alta resolução e estritamente crescente.

        O carro não envia timestamp, então o tempo é o da chegada na serial (inclui a
        latência do rádio). time.time_ns() puro tem resolução de ~15 ms no Windows: vários
        frames cairiam no MESMO instante e o InfluxDB os trataria como o mesmo ponto
        (sobrescrevendo valores). Por isso o relógio de parede é lido uma vez e o tempo
        avança com perf_counter_ns(); o +1 ns final garante unicidade mesmo assim.
        """
        ts = self._base_wall_ns + (time.perf_counter_ns() - self._base_perf_ns)
        if ts <= self._ultimo_ts_ns:
            ts = self._ultimo_ts_ns + 1
        self._ultimo_ts_ns = ts
        return ts

    @staticmethod
    def _ao_falhar(conf, dados, excecao) -> None:
        log.error("Lote descartado após as tentativas de reenvio ao InfluxDB: %s", excecao)

    @staticmethod
    def _ao_retentar(conf, dados, excecao) -> None:
        log.warning("Falha ao gravar no InfluxDB; nova tentativa agendada: %s", excecao)


# ============================================================================
# 4) LEITURA DA SERIAL + RECONEXÃO AUTOMÁTICA
# ============================================================================
def detectar_porta_esp32() -> Optional[str]:
    """Procura uma ponte USB-serial típica de ESP32. Reexecutada a cada reconexão porque
    o nome da porta pode mudar quando o cabo é recolocado (ex.: COM5 -> COM6)."""
    for porta in serial.tools.list_ports.comports():
        if porta.vid in VIDS_PONTES_USB:
            return porta.device
    return None


def _processar_e_gravar(linha: str, gravador: GravadorInflux, stats: Counter) -> None:
    try:
        resultado = processar_linha(linha, stats)
        if resultado is not None:
            gravador.gravar(*resultado)
    except Exception:  # noqa: BLE001 - um pacote ruim (ou bug) nunca pode derrubar a telemetria
        stats["erros_inesperados"] += 1
        log.exception("Erro inesperado ao tratar a linha %r (ignorada)", linha)


def ler_serial(ser: serial.Serial, gravador: GravadorInflux, stats: Counter) -> None:
    """Leitura contínua. Só termina por exceção (ex.: cabo USB removido)."""
    buffer = bytearray()
    proximo_relatorio = time.monotonic() + INTERVALO_RELATORIO_S

    while True:
        # Lê tudo que já chegou (ou 1 byte, esperando até TIMEOUT_SERIAL_S).
        # Cabo removido -> SerialException/OSError sobe para executar_telemetria().
        dados = ser.read(ser.in_waiting or 1)

        if dados:
            buffer.extend(dados)

            # RUÍDO (camada 1): buffer próprio em vez de ser.readline(). Se o timeout
            # estourar no meio de uma linha, readline() devolveria só um pedaço (que
            # seria descartado) e o resto chegaria como outra linha quebrada. Aqui o
            # pedaço fica guardado e é completado na leitura seguinte.
            *linhas_completas, buffer = buffer.split(b"\n")
            for bruto in linhas_completas:
                # ASCII estrito: byte >= 0x80 vira U+FFFD e a regex rejeita a linha.
                # Ao abrir a porta, DTR/RTS pode resetar a ESP32; o bootloader dela
                # cospe texto a 74880 bps, que aqui chega como lixo e cai neste filtro.
                _processar_e_gravar(bruto.decode("ascii", errors="replace"), gravador, stats)

            # RUÍDO (camada 1): lixo contínuo sem '\n' faria o buffer crescer sem limite.
            if len(buffer) > TAMANHO_MAX_LINHA:
                stats["descartes_buffer_estourado"] += 1
                buffer.clear()

        if time.monotonic() >= proximo_relatorio:
            log.info("Estatísticas: %s", dict(sorted(stats.items())))
            proximo_relatorio += INTERVALO_RELATORIO_S


def executar_telemetria(porta: Optional[str], gravador: GravadorInflux) -> None:
    """Mantém a telemetria viva: se a serial cair, volta a procurar a ESP32 e reconecta."""
    stats: Counter = Counter()
    while True:
        porta = porta or PORTA_SERIAL or detectar_porta_esp32()
        if porta is None:
            log.warning("ESP32 não encontrada. Nova tentativa em %.1f s...", INTERVALO_RECONEXAO_S)
        else:
            try:
                with serial.Serial(porta, BAUDRATE, timeout=TIMEOUT_SERIAL_S) as ser:
                    log.info("Conectado em %s (%d bps).", porta, BAUDRATE)
                    ser.reset_input_buffer()  # descarta bytes velhos acumulados antes da abertura
                    stats["conexoes"] += 1
                    ler_serial(ser, gravador, stats)
            except (serial.SerialException, OSError) as erro:
                log.error("Porta serial indisponível (%s): %s. Tentando reconectar...", porta, erro)
            porta = None  # na próxima volta, procura a porta de novo
        time.sleep(INTERVALO_RECONEXAO_S)


def rodar_telemetria_real(porta: str) -> None:
    if not INFLUX_TOKEN:
        log.error("Defina a variável de ambiente INFLUX_TOKEN antes de executar.")
        sys.exit(1)
    gravador = GravadorInflux(INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET)
    try:
        executar_telemetria(porta, gravador)
    except KeyboardInterrupt:
        log.info("Encerrando (Ctrl+C)...")
    finally:
        gravador.fechar()  # envia o último lote antes de sair


# ============================================================================
# 5) SIMULAÇÃO (MOCK DATA) - roda sem serial e sem InfluxDB
# ============================================================================
def _montar_linha(can_id: int, payload: Sequence[int]) -> str:
    return f"{can_id:08X}," + ",".join(str(b) for b in payload)


def _gerar_pacote_valido() -> tuple[str, dict[str, float]]:
    """Sorteia valores físicos e os codifica em bytes big-endian (inverso das equações).
    Devolve a linha serial e o "gabarito" com os valores que o parser deve recuperar."""
    can_id = random.choice((ID_RPM_TPS_MAP, ID_TEMPERATURAS_LAMBDA, ID_BATERIA))
    b = [random.randint(0, 255) for _ in range(8)]  # bytes sem uso: qualquer valor, devem ser ignorados

    if can_id == ID_RPM_TPS_MAP:
        rpm, tps, mapa = random.randint(800, 11_500), random.uniform(0, 100), random.uniform(0.2, 2.8)
        b[0], b[1] = divmod(rpm, 256)
        b[2] = round(tps * 2.55)
        b[3], b[4] = divmod(round(mapa * 1000) + 1000, 256)
        gabarito = {"rpm": rpm, "tps_pct": tps, "map_bar": mapa}
    elif can_id == ID_TEMPERATURAS_LAMBDA:
        t_motor, t_ar, lam = random.uniform(60, 110), random.uniform(20, 60), random.uniform(0.7, 1.2)
        b[0], b[1] = divmod(round(t_motor * 10), 256)
        b[2], b[3] = divmod(round(t_ar * 10), 256)
        b[4] = round((lam - 0.5) * 100)
        gabarito = {"temp_motor_c": t_motor, "temp_ar_c": t_ar, "lambda": lam}
    else:
        vbat = random.uniform(11.0, 14.8)
        b[0] = round(vbat * 10)
        gabarito = {"vbat_v": vbat}
    return _montar_linha(can_id, b), gabarito


def _corromper_linha(linha: str) -> str:
    """Reproduz os defeitos típicos de um enlace rádio + serial."""
    defeito = random.choice(
        ("truncada", "campo_perdido", "byte_invalido", "lixo", "boot_esp32", "id_desconhecido", "valor_absurdo")
    )
    campos = linha.split(",")
    if defeito == "truncada":  # porta aberta no meio de uma linha
        return linha[: random.randint(1, len(linha) - 1)]
    if defeito == "campo_perdido":
        campos.pop(random.randint(1, 8))
    elif defeito == "byte_invalido":
        campos[random.randint(1, 8)] = random.choice(("256", "999", "-3", "x7", "", "1.5"))
    elif defeito == "lixo":  # bytes aleatórios (EMI, baud rate errado...)
        return "".join(chr(random.randint(0, 255)) for _ in range(random.randint(5, 40)))
    elif defeito == "boot_esp32":
        return "ets Jun  8 2016 00:22:57 rst:0x1 (POWERON_RESET)"
    elif defeito == "valor_absurdo":  # formato perfeito, mas B0=255 é impossível (65280 rpm, 25,5 V...)
        campos[1] = "255"
    else:  # id_desconhecido
        campos[0] = f"18FF{random.randint(0, 0xFFFF):04X}"
    return ",".join(campos)


# Resolução de cada variável (menor passo que o byte consegue representar)
_RESOLUCAO = {"rpm": 1, "tps_pct": 1 / 2.55, "map_bar": 0.001, "temp_motor_c": 0.1,
              "temp_ar_c": 0.1, "lambda": 0.01, "vbat_v": 0.1}


def _confere(obtido: Optional[dict], esperado: Optional[dict], tolerancia: Optional[dict] = None) -> bool:
    if esperado is None or obtido is None:
        return obtido is esperado
    if obtido.keys() != esperado.keys():
        return False
    return all(
        math.isclose(obtido[k], esperado[k], abs_tol=(tolerancia or {}).get(k, 1e-9)) for k in esperado
    )


def _formatar(valores: Optional[dict]) -> str:
    return "descartada" if valores is None else " ".join(f"{k}={v:.2f}" for k, v in valores.items())


def simular_pacotes(qtd_linhas: int = 300, taxa_ruido: float = 0.25) -> bool:
    """Testa o pipeline inteiro (formato -> parser -> sanity check) sem serial e sem InfluxDB.
    Devolve True se todos os testes passaram."""
    ok = True
    print("\n=== SIMULAÇÃO (mock data) - nada é gravado no InfluxDB ===")

    # --- 1) Vetores de teste com resposta conhecida (calculada à mão) -------------------------
    print("\n[1/2] Vetores de teste do parser")
    vetores = [
        ("14000000,19,136,128,7,208,0,0,0", {"rpm": 5000.0, "tps_pct": 128 / 2.55, "map_bar": 1.0}),
        ("14000001,3,232,1,244,50,0,0,0", {"temp_motor_c": 100.0, "temp_ar_c": 50.0, "lambda": 1.0}),
        ("14000002,138,0,0,0,0,0,0,0\r", {"vbat_v": 13.8}),                  # '\r' do fim de linha
        ("14000000,255,255,128,0,0,0,0,0", {"tps_pct": 128 / 2.55}),         # RPM=65535 e MAP=-1 bar: descartados
        ("14000002,0,0,0,0,0,0,0,0", None),                                  # 0 V: fora de [8, 16]
        ("18FF1234,1,2,3,4,5,6,7,8", None),                                  # ID desconhecido
        ("14000000,19,136,128", None),                                       # linha truncada
        ("14000000,19,136,300,7,208,0,0,0", None),                           # byte > 255
        ("ets Jun  8 2016 00:22:57", None),                                  # log de boot da ESP32
        ("\ufffd\ufffd\ufffd", None),                                        # lixo não-ASCII
    ]
    for linha, esperado in vetores:
        resultado = processar_linha(linha, Counter())
        obtido = resultado[1] if resultado else None
        passou = _confere(obtido, esperado)
        ok = ok and passou
        print(f"  [{'OK' if passou else 'FALHA'}] {linha.strip()!r:<38} -> {_formatar(obtido)}")

    # --- 2) Fluxo aleatório com ruído injetado -------------------------------------------------
    print(f"\n[2/2] Fluxo aleatório: {qtd_linhas} linhas, ~{taxa_ruido:.0%} corrompidas")
    stats: Counter = Counter()
    divergencias = vazamentos = 0
    amostras: list[str] = []

    for _ in range(qtd_linhas):
        linha, gabarito = _gerar_pacote_valido()
        corrompida = random.random() < taxa_ruido
        if corrompida:
            linha = _corromper_linha(linha)

        resultado = processar_linha(linha, stats)
        if resultado is None:
            divergencias += 0 if corrompida else 1  # pacote íntegro nunca pode ser descartado
            continue

        can_id, valores = resultado
        if len(amostras) < 6:
            amostras.append(f"  {can_id:08X} -> {_formatar(valores)}")
        # Invariante do sanity check: nada fora da faixa pode chegar ao banco
        vazamentos += sum(not (LIMITES_SANIDADE[k][0] <= v <= LIMITES_SANIDADE[k][1]) for k, v in valores.items())
        # Pacote íntegro: o parser tem de recuperar o valor original (erro máx. = meia resolução)
        if not corrompida:
            tolerancia = {k: _RESOLUCAO[k] / 2 + 1e-9 for k in gabarito}
            divergencias += 0 if _confere(valores, gabarito, tolerancia) else 1

    print("  Amostras decodificadas:")
    print("\n".join(amostras))
    print(f"  Contadores do filtro: {dict(sorted(stats.items()))}")
    print(f"  Divergências do parser: {divergencias} | Valores fora da faixa que passaram: {vazamentos}")
    ok = ok and divergencias == 0 and vazamentos == 0

    print(f"\n>>> RESULTADO DA SIMULAÇÃO: {'APROVADO' if ok else 'REPROVADO'}")
    return ok


# ============================================================================
# PONTO DE ENTRADA
# ============================================================================
if __name__ == "__main__":
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    
    # Vamos instanciar o gravador para ele mandar para o InfluxDB
    if not INFLUX_TOKEN:
        log.error("Defina a variável de ambiente INFLUX_TOKEN antes de executar.")
        sys.exit(1)
        
    gravador = GravadorInflux(INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET)
    stats: Counter = Counter()

    print("\n=== MODO SIMULAÇÃO CONTÍNUA (Gerando Mock Data para o InfluxDB) ===")
    print("Pressione Ctrl+C para encerrar.\n")

    try:
        while True:
            # 1. Gera um pacote válido e simula o envio pela serial
            linha_simulada, _ = _gerar_pacote_valido()
            
            # 2. Processa a linha com o parser e envia para o InfluxDB em lote
            _processar_e_gravar(linha_simulada, gravador, stats)
            
            # 3. Espera 100 ms (simulando a taxa de 10 Hz da telemetria real)
            time.sleep(0.1)
            
    except KeyboardInterrupt:
        log.info("Encerrando simulação (Ctrl+C)...")
    finally:
        gravador.fechar()