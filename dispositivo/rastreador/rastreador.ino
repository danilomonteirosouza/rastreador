/**
 * PROJETO PRINCIPAL - LILYGO TTGO T-Call A7670E V1.0
 *
 * Versão com:
 *  - supervisão contínua de conectividade
 *  - failover 4G <-> Wi-Fi
 *  - MQTT via Wi-Fi com WiFiClientSecure + PubSubClient
 *  - MQTT via 4G com MQTT(S) nativo do modem A7670E
 *  - logs detalhados
 *  - envio de GPS em máquina de estados
 *  - prioridade máxima para mensagem MQTT recebida
 *  - ACK MQTT ao receber e executar comando
 *  - preparo do rádio antes do registro da rede
 *  - seleção automática de operadora antes do attach
 *  - recuperação do rádio ao detectar CSQ=99
 *  - limpeza extra do estado de rede antes da APN
 *  - boot com salto direto para Wi-Fi se o SIM não estiver pronto
 *  - failover imediato para Wi-Fi ao detectar remoção do SIM em runtime
 *  - debounce para remoção e reinserção do SIM
 *  - retorno automático ao 4G quando o SIM reaparecer durante uso do Wi-Fi
 *
 * Observação:
 *  - No Wi-Fi: mantém fluxo clássico com PubSubClient
 *  - No 4G: usa AT+CMQTT* com TLS nativo do modem
 */

#define TINY_GSM_RX_BUFFER 2048
#define TINY_GSM_MODEM_A7670

#include "utilities.h"
#include <TinyGsmClient.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <PubSubClient.h>
#include "Arduino.h"

#define DUMP_AT_COMMANDS

#ifdef DUMP_AT_COMMANDS
  #include <StreamDebugger.h>
  StreamDebugger debugger(SerialAT, Serial);
  TinyGsm modem(debugger);
#else
  TinyGsm modem(SerialAT);
#endif

// ------------------------------------------------------------
// SAIDAS DO PROJETO
// ------------------------------------------------------------
#define MOTORES 17
#define LED     33

// ------------------------------------------------------------
// WIFI FALLBACK
// ------------------------------------------------------------
const char* ssid = "FamilyLDD";
const char* password = "JOAO0316@@!";

// ------------------------------------------------------------
// MQTT / HIVE MQ CLOUD
// ------------------------------------------------------------
const char* mqtt_server = "231ccd91865148f78345c07e2d7e799e.s2.eu.hivemq.cloud";
const int   mqtt_server_port = 8883;
const char* hiveIOTUser = "rastreador";
const char* hiveIOTPassword = "Extranet1";

const char* topicBasicSensors = "rastreador";
const char* topicNameStablishConnection = "firstAttemptConnection";
const char* messageOnceStablishConnection = "Communication working properly";
const char* topicAck = "rastreador/ack";

// ------------------------------------------------------------
// CERTIFICADO ROOT CA
// ------------------------------------------------------------
static const char *root_ca PROGMEM = R"EOF(
-----BEGIN CERTIFICATE-----
MIIFazCCA1OgAwIBAgIRAIIQz7DSQONZRGPgu2OCiwAwDQYJKoZIhvcNAQELBQAw
TzELMAkGA1UEBhMCVVMxKTAnBgNVBAoTIEludGVybmV0IFNlY3VyaXR5IFJlc2Vh
cmNoIEdyb3VwMRUwEwYDVQQDEwxJU1JHIFJvb3QgWDEwHhcNMTUwNjA0MTEwNDM4
WhcNMzUwNjA0MTEwNDM4WjBPMQswCQYDVQQGEwJVUzEpMCcGA1UEChMgSW50ZXJu
ZXQgU2VjdXJpdHkgUmVzZWFyY2ggR3JvdXAxFTATBgNVBAMTDElTUkcgUm9vdCBY
MTCCAiIwDQYJKoZIhvcNAQEBBQADggIPADCCAgoCggIBAK3oJHP0FDfzm54rVygc
h77ct984kIxuPOZXoHj3dcKi/vVqbvYATyjb3miGbESTtrFj/RQSa78f0uoxmyF+
0TM8ukj13Xnfs7j/EvEhmkvBioZxaUpmZmyPfjxwv60pIgbz5MDmgK7iS4+3mX6U
A5/TR5d8mUgjU+g4rk8Kb4Mu0UlXjIB0ttov0DiNewNwIRt18jA8+o+u3dpjq+sW
T8KOEUt+zwvo/7V3LvSye0rgTBIlDHCNAymg4VMk7BPZ7hm/ELNKjD+Jo2FR3qyH
B5T0Y3HsLuJvW5iB4YlcNHlsdu87kGJ55tukmi8mxdAQ4Q7e2RCOFvu396j3x+UC
B5iPNgiV5+I3lg02dZ77DnKxHZu8A/lJBdiB3QW0KtZB6awBdpUKD9jf1b0SHzUv
KBds0pjBqAlkd25HN7rOrFleaJ1/ctaJxQZBKT5ZPt0m9STJEadao0xAH0ahmbWn
OlFuhjuefXKnEgV4We0+UXgVCwOPjdAvBbI+e0ocS3MFEvzG6uBQE3xDk3SzynTn
jh8BCNAw1FtxNrQHusEwMFxIt4I7mKZ9YIqioymCzLq9gwQbooMDQaHWBfEbwrbw
qHyGO0aoSCqI3Haadr8faqU9GY/rOPNk3sgrDQoo//fb4hVC1CLQJ13hef4Y53CI
rU7m2Ys6xt0nUW7/vGT1M0NPAgMBAAGjQjBAMA4GA1UdDwEB/wQEAwIBBjAPBgNV
HRMBAf8EBTADAQH/MB0GA1UdDgQWBBR5tFnme7bl5AFzgAiIyBpY9umbbjANBgkq
hkiG9w0BAQsFAAOCAgEAVR9YqbyyqFDQDLHYGmkgJykIrGF1XIpu+ILlaS/V9lZL
ubhzEFnTIZd+50xx+7LSYK05qAvqFyFWhfFQDlnrzuBZ6brJFe+GnY+EgPbk6ZGQ
3BebYhtF8GaV0nxvwuo77x/Py9auJ/GpsMiu/X1+mvoiBOv/2X/qkSsisRcOj/KK
NFtY2PwByVS5uCbMiogziUwthDyC3+6WVwW6LLv3xLfHTjuCvjHIInNzktHCgKQ5
ORAzI4JMPJ+GslWYHb4phowim57iaztXOoJwTdwJx4nLCgdNbOhdjsnvzqvHu7Ur
TkXWStAmzOVyyghqpZXjFaH3pO3JLF+l+/+sKAIuvtd7u+Nxe5AW0wdeRlN8NwdC
jNPElpzVmbUq4JUagEiuTDkHzsxHpFKVK7q4+63SM1N95R1NbdWhscdCb+ZAJzVc
oyi3B43njTOQ5yOf+1CceWxG1bQVs5ZufpsMljq4Ui0/1lvh+wjChP4kqKOJ2qxq
4RgqsahDYVvTH9w7jXbyLeiNdd8XM2w9U/t7y0Ff/9yi0GE44Za4rF2LN9d11TPA
mRGunUHBcnWEvgJBQl9nJEiU0Zsnvgc/ubhPgXRR4Xq37Z0j4r7g1SgEEzwxA57d
emyPxgcYxn/eR44/KJ4EBs+lVDR3veyJm+kXQ99b21/+jh5Xos1AnX5iItreGCc=
-----END CERTIFICATE-----
)EOF";

static const char* MODEM_CA_FILENAME = "hivemq_ca.pem";

// ------------------------------------------------------------
// APNS
// ------------------------------------------------------------
struct ApnConfig {
  const char* operadora;
  const char* apn;
  const char* user;
  const char* pass;
};

ApnConfig apns[] = {
  { "VIVO",  "zap.vivo.com.br",   "vivo",  "vivo"  },
  { "TIM",   "timbrasil.br",      "tim",   "tim"   },
  { "CLARO", "java.claro.com.br", "Claro", "Claro" }
};

const int APN_COUNT = sizeof(apns) / sizeof(apns[0]);

// ------------------------------------------------------------
// CLIENTES
// ------------------------------------------------------------
TinyGsmClient gsmClient(modem);
WiFiClientSecure wifiClient;
PubSubClient wifiMqttClient;

// ------------------------------------------------------------
// ESTADO
// ------------------------------------------------------------
bool usingGsm = false;
bool usingWifi = false;
bool systemReady = false;

bool gsmMqttConnected = false;
bool gsmMqttServiceStarted = false;
bool gsmMqttClientAcquired = false;
bool gsmCertReady = false;

unsigned long lastGpsPublish = 0;
const unsigned long gpsInterval = 60000UL;

unsigned long lastNetworkCheck = 0;
const unsigned long networkCheckInterval = 15000UL;

unsigned long lastMqttReconnectAttempt = 0;
const unsigned long mqttReconnectInterval = 5000UL;

unsigned long lastCellularRecoveryAttempt = 0;
const unsigned long cellularRecoveryInterval = 20000UL;

unsigned long lastWifiRecoveryAttempt = 0;
const unsigned long wifiRecoveryInterval = 15000UL;

unsigned long lastSimPoll = 0;
const unsigned long simPollInterval = 4000UL;

unsigned long lastCellularReturnAttempt = 0;
const unsigned long cellularReturnInterval = 30000UL;

#define MSG_BUFFER_SIZE 512
char bufferMessage[MSG_BUFFER_SIZE];

// ------------------------------------------------------------
// PRIORIDADE MQTT
// ------------------------------------------------------------
volatile bool mqttPriorityActive = false;
volatile unsigned long mqttPriorityUntil = 0;
const unsigned long mqttPriorityHoldMs = 5000UL;

// ------------------------------------------------------------
// ESTADO DE DETECCAO DO SIM
// ------------------------------------------------------------
int simAbsentConfirmCount = 0;
int simReadyConfirmCount = 0;
const int simAbsentConfirmThreshold = 2;
const int simReadyConfirmThreshold = 2;
bool simRemovalLatched = false;

// ------------------------------------------------------------
// MAQUINA DE ESTADOS DO GPS
// ------------------------------------------------------------
enum GpsPublishState {
  GPS_IDLE = 0,
  GPS_POWER_ON,
  GPS_WAIT_BEFORE_READ,
  GPS_READ_ATTEMPT,
  GPS_PUBLISH
};

GpsPublishState gpsState = GPS_IDLE;
unsigned long gpsNextReadAt = 0;
int gpsAttemptCount = 0;
const int gpsMaxAttempts = 60;
const unsigned long gpsInitialWaitMs = 30000UL;
const unsigned long gpsRetryWaitMs = 5000UL;
double gpsLat = 0.0;
double gpsLon = 0.0;

// ------------------------------------------------------------
// RX MQTT NATIVO DO MODEM
// ------------------------------------------------------------
struct GsmMqttRxState {
  bool receiving = false;
  bool expectingTopicData = false;
  bool expectingPayloadData = false;
  int topicTotalLen = 0;
  int payloadTotalLen = 0;
  String topic;
  String payload;
} gsmRx;

// ------------------------------------------------------------
// LOG
// ------------------------------------------------------------
String nowTag() {
  return "[" + String(millis()) + " ms] ";
}

void logInfo(const String &msg) {
  Serial.println(nowTag() + "[INFO] " + msg);
}

void logOk(const String &msg) {
  Serial.println(nowTag() + "[OK] " + msg);
}

void logWarn(const String &msg) {
  Serial.println(nowTag() + "[WARN] " + msg);
}

void logErr(const String &msg) {
  Serial.println(nowTag() + "[ERRO] " + msg);
}

void logStep(const String &msg) {
  Serial.println();
  Serial.println(nowTag() + "================ " + msg + " ================");
}

// ------------------------------------------------------------
// UTILITARIOS
// ------------------------------------------------------------
String simStatusToString(int simStatus) {
  switch (simStatus) {
    case 0: return "DESCONHECIDO/NAO PRONTO";
    case 1: return "SIM AUSENTE";
    case 2: return "PIN NECESSARIO";
    case 3: return "SIM PRONTO";
    case 4: return "PUK NECESSARIO";
    default: return "STATUS " + String(simStatus);
  }
}

String regStatusToString(int reg) {
  switch (reg) {
    case 0: return "NAO REGISTRADO";
    case 1: return "REGISTRADO_HOME";
    case 2: return "PROCURANDO_REDE";
    case 3: return "REGISTRO_NEGADO";
    case 4: return "STATUS_DESCONHECIDO";
    case 5: return "REGISTRADO_ROAMING";
    default: return "REG=" + String(reg);
  }
}

String csqToDbmString(int csq) {
  if (csq == 99) return "desconhecido";
  int dbm = -113 + (2 * csq);
  return String(dbm) + " dBm";
}

void activateMqttPriority() {
  mqttPriorityActive = true;
  mqttPriorityUntil = millis() + mqttPriorityHoldMs;
}

void refreshMqttPriorityWindow() {
  if (!mqttPriorityActive) return;
  if ((long)(millis() - mqttPriorityUntil) >= 0) {
    mqttPriorityActive = false;
  }
}

bool shouldPauseNonCriticalTasks() {
  refreshMqttPriorityWindow();
  return mqttPriorityActive;
}

bool isSimOperationalStatus(int simStatus) {
  return simStatus == 3;
}

bool isSimAbsentStatus(int simStatus) {
  return simStatus == 1 || simStatus == 0 || simStatus == 2 || simStatus == 4;
}

// ------------------------------------------------------------
// MQTT LOOP SAFE
// ------------------------------------------------------------
void mqttLoopSafe() {
  if (!systemReady) return;

  if (usingWifi && wifiMqttClient.connected()) {
    wifiMqttClient.loop();
  }
}

// ------------------------------------------------------------
// SERIAL AT
// ------------------------------------------------------------
void flushSerialAT() {
  while (SerialAT.available()) {
    SerialAT.read();
  }
}

bool waitForResponseContains(String &resp, uint32_t timeoutMs, const String &needle1, const String &needle2 = "") {
  unsigned long start = millis();
  resp = "";

  while (millis() - start < timeoutMs) {
    while (SerialAT.available()) {
      char c = (char)SerialAT.read();
      resp += c;

      if (needle1.length() && resp.indexOf(needle1) >= 0) return true;
      if (needle2.length() && resp.indexOf(needle2) >= 0) return true;
    }

    mqttLoopSafe();
    refreshMqttPriorityWindow();
    yield();
  }

  return false;
}

bool sendATExpect(const String& cmd, const String& expect1, uint32_t timeoutMs, String *fullResp = nullptr, const String& expect2 = "") {
  flushSerialAT();

  logInfo("Enviando AT: " + cmd);
  SerialAT.println(cmd);

  String resp;
  bool ok = waitForResponseContains(resp, timeoutMs, expect1, expect2);

  String logResp = resp;
  logResp.replace("\r", " ");
  logResp.replace("\n", " | ");
  if (logResp.length()) {
    logInfo(cmd + " => " + logResp);
  } else {
    logWarn("Sem resposta visível para " + cmd);
  }

  if (fullResp) *fullResp = resp;
  return ok;
}

bool sendATExpectOk(const String& cmd, uint32_t timeoutMs = 5000, String *fullResp = nullptr) {
  return sendATExpect(cmd, "OK", timeoutMs, fullResp, "ERROR");
}

bool sendATExpectPrompt(const String& cmd, uint32_t timeoutMs = 5000, String *fullResp = nullptr) {
  return sendATExpect(cmd, ">", timeoutMs, fullResp, "ERROR");
}

bool sendRawAfterPrompt(const String& data, uint32_t timeoutMs = 10000, String *fullResp = nullptr) {
  logInfo("Enviando payload de " + String(data.length()) + " bytes");
  SerialAT.write((const uint8_t*)data.c_str(), data.length());

  String resp;
  bool ok = waitForResponseContains(resp, timeoutMs, "OK", "ERROR");

  String logResp = resp;
  logResp.replace("\r", " ");
  logResp.replace("\n", " | ");
  if (logResp.length()) {
    logInfo("Payload => " + logResp);
  }

  if (fullResp) *fullResp = resp;
  return ok;
}

void dumpModemOutput(uint32_t windowMs = 20) {
  unsigned long start = millis();
  String raw = "";

  while (millis() - start < windowMs) {
    while (SerialAT.available()) {
      char c = (char)SerialAT.read();
      raw += c;
    }

    mqttLoopSafe();
    refreshMqttPriorityWindow();
    yield();
  }

  raw.trim();
  if (raw.length() > 0) {
    raw.replace("\r", " ");
    raw.replace("\n", " | ");
    logInfo("URC/AT espontâneo => " + raw);
  }
}

// ------------------------------------------------------------
// AUTOBAUD
// ------------------------------------------------------------
uint32_t AutoBaud() {
  static uint32_t rates[] = {115200, 9600, 57600, 38400, 19200, 74400, 74880,
                             230400, 460800, 2400, 4800, 14400, 28800};

  for (uint8_t i = 0; i < sizeof(rates) / sizeof(rates[0]); i++) {
    uint32_t rate = rates[i];
    logInfo("Tentando baud rate " + String(rate));
    SerialAT.updateBaudRate(rate);
    delay(10);

    for (int j = 0; j < 10; j++) {
      flushSerialAT();
      SerialAT.print("AT\r\n");

      String input;
      waitForResponseContains(input, 800, "OK", "ERROR");

      if (input.indexOf("OK") >= 0) {
        input.replace("\r", " ");
        input.replace("\n", " | ");
        logOk("Modem respondeu em " + String(rate));
        logInfo("Resposta AT: " + input);
        return rate;
      }
    }
  }

  SerialAT.updateBaudRate(115200);
  return 0;
}

// ------------------------------------------------------------
// CALLBACK WIFI MQTT
// ------------------------------------------------------------
void wifi_mqtt_callback(char* topic, byte* payload, unsigned int length);

// ------------------------------------------------------------
// MQTT COMANDO
// ------------------------------------------------------------
bool publishAck(const String& command, const String& status, const String& detail);

void handleIncomingCommand(const String& topic, const String& payload) {
  Serial.println(nowTag() + "[MQTT] Mensagem recebida no tópico [" + topic + "]: " + payload);

  activateMqttPriority();
  logWarn("Mensagem MQTT recebeu prioridade máxima");

  bool commandRecognized = true;
  bool commandApplied = true;
  String detail = "executado";

  if (payload == "ligarMotor") {
    digitalWrite(MOTORES, HIGH);
    logOk("Comando aplicado: motor ligado");
  } else if (payload == "desligarMotor") {
    digitalWrite(MOTORES, LOW);
    logOk("Comando aplicado: motor desligado");
  } else if (payload == "ligarLed") {
    digitalWrite(LED, LOW);
    logOk("Comando aplicado: LED externo ligado");
  } else if (payload == "desligarLed") {
    digitalWrite(LED, HIGH);
    logOk("Comando aplicado: LED externo desligado");
  } else {
    commandRecognized = false;
    commandApplied = false;
    detail = "comando nao cadastrado";
    logWarn("Comando recebido, mas sem ação cadastrada");
  }

  if (commandRecognized && commandApplied) {
    publishAck(payload, "ok", detail);
  } else {
    publishAck(payload, "ignored", detail);
  }
}

void wifi_mqtt_callback(char* topic, byte* payload, unsigned int length) {
  String topicStr(topic);
  String payloadStr;

  for (unsigned int i = 0; i < length; i++) {
    payloadStr += (char)payload[i];
  }

  handleIncomingCommand(topicStr, payloadStr);
}

// ------------------------------------------------------------
// PINOS
// ------------------------------------------------------------
void setupPins() {
  pinMode(MOTORES, OUTPUT);
  pinMode(LED, OUTPUT);

#ifdef BOARD_POWERON_PIN
  pinMode(BOARD_POWERON_PIN, OUTPUT);
  digitalWrite(BOARD_POWERON_PIN, HIGH);
#endif

  pinMode(MODEM_RESET_PIN, OUTPUT);
  pinMode(BOARD_PWRKEY_PIN, OUTPUT);
  pinMode(MODEM_DTR_PIN, OUTPUT);

  digitalWrite(MOTORES, LOW);
  digitalWrite(LED, HIGH);
  digitalWrite(MODEM_DTR_PIN, LOW);

  logOk("Pinos configurados");
}

// ------------------------------------------------------------
// BOOT MODEM
// ------------------------------------------------------------
bool initModem() {
  logStep("INICIALIZACAO DO MODEM");

  SerialAT.begin(115200, SERIAL_8N1, MODEM_RX_PIN, MODEM_TX_PIN);
  logInfo("SerialAT iniciada");

  digitalWrite(MODEM_RESET_PIN, !MODEM_RESET_LEVEL);
  delay(100);
  digitalWrite(MODEM_RESET_PIN, MODEM_RESET_LEVEL);
  delay(2600);
  digitalWrite(MODEM_RESET_PIN, !MODEM_RESET_LEVEL);

  digitalWrite(BOARD_PWRKEY_PIN, LOW);
  delay(100);
  digitalWrite(BOARD_PWRKEY_PIN, HIGH);
  delay(100);
  digitalWrite(BOARD_PWRKEY_PIN, LOW);

  delay(1500);

  uint32_t foundBaud = AutoBaud();
  if (!foundBaud) {
    logErr("Falha no AutoBaud");
    return false;
  }

  logOk("AutoBaud concluído em " + String(foundBaud));
  delay(500);

  String modemInfo = modem.getModemInfo();
  if (modemInfo.length() > 0) {
    logInfo("Informações do modem: " + modemInfo);
  } else {
    logWarn("Não foi possível ler getModemInfo()");
  }

  return true;
}

// ------------------------------------------------------------
// SIM
// ------------------------------------------------------------
int getSimStatusFromAT() {
  String resp;

  if (!sendATExpectOk("AT+CPIN?", 1200, &resp)) {
    logWarn("Falha ao consultar status do SIM por AT+CPIN?");
    return 0;
  }

  String upper = resp;
  upper.toUpperCase();

  if (upper.indexOf("READY") >= 0) return 3;
  if (upper.indexOf("SIM REMOVED") >= 0) return 1;
  if (upper.indexOf("SIM PIN") >= 0 || upper.indexOf("PIN REQUIRED") >= 0) return 2;
  if (upper.indexOf("SIM PUK") >= 0 || upper.indexOf("PUK REQUIRED") >= 0) return 4;

  return 0;
}

bool waitForSimReady(unsigned long timeoutMs = 60000UL) {
  logStep("VERIFICACAO DO SIM");

  unsigned long start = millis();
  int lastStatus = -1;

  while (millis() - start < timeoutMs) {
    int simStatus = getSimStatusFromAT();

    if (simStatus != lastStatus) {
      logInfo("SIM status: " + simStatusToString(simStatus));
      lastStatus = simStatus;
    }

    if (simStatus == 3) {
      logOk("SIM card pronto");
      return true;
    }

    if (simStatus == 2) logErr("SIM bloqueado por PIN");
    if (simStatus == 1) logWarn("SIM ausente ou não detectado");

    delay(600);
  }

  logWarn("Tempo esgotado aguardando SIM card");
  return false;
}

bool detectConfirmedSimRemoval() {
  if (millis() - lastSimPoll < simPollInterval) {
    return simRemovalLatched;
  }

  lastSimPoll = millis();
  int simStatus = getSimStatusFromAT();

  if (isSimAbsentStatus(simStatus)) {
    simAbsentConfirmCount++;
    simReadyConfirmCount = 0;

    logWarn("Leitura SIM para remoção: " + simStatusToString(simStatus) +
            " | contador=" + String(simAbsentConfirmCount) + "/" + String(simAbsentConfirmThreshold));

    if (simAbsentConfirmCount >= simAbsentConfirmThreshold) {
      if (!simRemovalLatched) {
        logErr("Remoção do SIM confirmada");
      }
      simRemovalLatched = true;
      return true;
    }
  } else {
    simAbsentConfirmCount = 0;
    if (simRemovalLatched) {
      logInfo("Leitura SIM voltou a READY, aguardando confirmação de retorno");
    }
  }

  return simRemovalLatched;
}

bool detectConfirmedSimReady() {
  if (millis() - lastSimPoll < simPollInterval) {
    return (!simRemovalLatched && simReadyConfirmCount >= simReadyConfirmThreshold);
  }

  lastSimPoll = millis();
  int simStatus = getSimStatusFromAT();

  if (isSimOperationalStatus(simStatus)) {
    simReadyConfirmCount++;
    simAbsentConfirmCount = 0;

    logInfo("Leitura SIM para retorno: " + simStatusToString(simStatus) +
            " | contador=" + String(simReadyConfirmCount) + "/" + String(simReadyConfirmThreshold));

    if (simReadyConfirmCount >= simReadyConfirmThreshold) {
      if (simRemovalLatched) {
        logOk("Retorno do SIM confirmado");
      }
      simRemovalLatched = false;
      return true;
    }
  } else {
    simReadyConfirmCount = 0;
  }

  return false;
}

void resetSimPresenceDebounce() {
  simAbsentConfirmCount = 0;
  simReadyConfirmCount = 0;
  simRemovalLatched = false;
}

// ------------------------------------------------------------
// REDE APOIO
// ------------------------------------------------------------
bool configureRadioBeforeRegistration() {
  logStep("PREPARO DO RADIO");

  bool ok = true;

  if (!sendATExpectOk("AT+CFUN=1", 3000)) {
    logWarn("AT+CFUN=1 sem confirmação de OK");
    ok = false;
  }
  delay(800);

  if (!sendATExpectOk("AT+CREG=2", 1500)) {
    logWarn("AT+CREG=2 não confirmado");
  }

  if (!sendATExpectOk("AT+CGREG=2", 1500)) {
    logWarn("AT+CGREG=2 não confirmado");
  }

  if (!sendATExpectOk("AT+CEREG=2", 1500)) {
    logWarn("AT+CEREG=2 não confirmado");
  }

  if (!sendATExpectOk("AT+CNMP=2", 1500)) {
    logWarn("AT+CNMP=2 não aceito por este firmware");
  }

  sendATExpectOk("AT+CFUN?", 1500);
  sendATExpectOk("AT+CNMP?", 1500);
  sendATExpectOk("AT+COPS?", 2500);

  return ok;
}

bool forceAutomaticOperatorSelection() {
  logStep("SELECAO DE OPERADORA");

  logInfo("Solicitando seleção automática de operadora");
  bool ok = sendATExpectOk("AT+COPS=0", 15000);

  if (!ok) {
    logWarn("AT+COPS=0 sem OK. O fluxo seguirá com diagnóstico extra.");
  }

  delay(1500);
  sendATExpectOk("AT+COPS?", 2500);
  return ok;
}

void disconnectCellularData() {
  logInfo("Desativando sessão de dados móveis anterior");
  modem.gprsDisconnect();
  delay(1000);
}

bool cleanNetworkStateBeforeApn() {
  logStep("LIMPEZA DE ESTADO DE REDE");

  bool anyOk = false;

  disconnectCellularData();

  if (sendATExpectOk("AT+CGATT=0", 8000)) {
    logOk("Detach de pacote executado");
    anyOk = true;
  } else {
    logWarn("AT+CGATT=0 sem confirmação");
  }

  if (sendATExpectOk("AT+CGACT=0,1", 5000)) {
    logOk("PDP context CID 1 desativado");
    anyOk = true;
  } else {
    logWarn("AT+CGACT=0,1 sem confirmação");
  }

  if (sendATExpectOk("AT+COPS=2", 15000)) {
    logOk("Deregister solicitado");
    anyOk = true;
  } else {
    logWarn("AT+COPS=2 sem confirmação");
  }

  delay(1200);

  sendATExpectOk("AT+CGATT?", 1200);
  sendATExpectOk("AT+CGACT?", 2000);
  sendATExpectOk("AT+COPS?", 2500);
  sendATExpectOk("AT+CGPADDR", 2000);

  return anyOk;
}

bool reattachPacketDomain() {
  logStep("REATTACH DE PACOTE");

  if (!sendATExpectOk("AT+CGATT=1", 10000)) {
    logWarn("AT+CGATT=1 sem confirmação de OK");
    sendATExpectOk("AT+CGATT?", 1500);
    return false;
  }

  delay(1500);
  sendATExpectOk("AT+CGATT?", 1500);
  return true;
}

bool recoverRadioFromUnknownSignal() {
  logStep("RECUPERACAO DO RADIO POR CSQ=99");

  bool ok = true;

  logWarn("CSQ=99 persistente. Reiniciando pilha de rádio.");

  if (!sendATExpectOk("AT+CFUN=0", 5000)) {
    logWarn("AT+CFUN=0 sem confirmação");
    ok = false;
  }

  delay(2500);

  if (!sendATExpectOk("AT+CFUN=1", 8000)) {
    logWarn("AT+CFUN=1 sem confirmação");
    ok = false;
  }

  delay(4000);

  forceAutomaticOperatorSelection();
  return ok;
}

// ------------------------------------------------------------
// REDE
// ------------------------------------------------------------
void logNetworkSnapshot() {
  int simStatus = getSimStatusFromAT();
  int16_t csq = modem.getSignalQuality();
  int reg = modem.getRegistrationStatus();

  logInfo("SIM: " + simStatusToString(simStatus));
  logInfo("CSQ: " + String(csq) + " (" + csqToDbmString(csq) + ")");
  logInfo("Registro: " + regStatusToString(reg));
}

bool isCellularLinkHealthy() {
  int simStatus = getSimStatusFromAT();

  if (simStatus != 3) {
    logErr("SIM removido ou inválido durante operação 4G");
    return false;
  }

  int reg = modem.getRegistrationStatus();
  bool gprs = modem.isGprsConnected();

  logInfo("Saúde 4G -> SIM=" + simStatusToString(simStatus) +
          " | Registro=" + regStatusToString(reg) +
          " | GPRS=" + String(gprs ? "CONECTADO" : "DESCONECTADO"));

  return (reg == 1 || reg == 5) && gprs;
}

bool waitForNetworkRegistration(unsigned long timeoutMs = 60000UL) {
  logStep("REGISTRO NA REDE MOVEL");

  int simStatusAtStart = getSimStatusFromAT();
  if (simStatusAtStart != 3) {
    logWarn("Registro 4G abortado. SIM não está pronto.");
    return false;
  }

  configureRadioBeforeRegistration();
  forceAutomaticOperatorSelection();

  unsigned long start = millis();
  int csq99Count = 0;
  int lastReg = -999;
  int lastCsq = -999;

  while (millis() - start < timeoutMs) {
    if (shouldPauseNonCriticalTasks()) {
      logWarn("Registro na rede pausado por prioridade MQTT");
      return false;
    }

    int simStatus = getSimStatusFromAT();
    if (simStatus != 3) {
      logErr("SIM deixou de estar pronto durante o registro da rede");
      return false;
    }

    int16_t csq = modem.getSignalQuality();
    int reg = modem.getRegistrationStatus();

    if (csq != lastCsq || reg != lastReg) {
      logInfo("Sinal CSQ=" + String(csq) + " (" + csqToDbmString(csq) + ")" +
              " | Registro=" + regStatusToString(reg));
      lastCsq = csq;
      lastReg = reg;
    }

    if (reg == 1 || reg == 5) {
      logOk("Registro na rede móvel concluído");
      logNetworkSnapshot();
      sendATExpectOk("AT+COPS?", 2500);
      return true;
    }

    if (csq == 99) {
      csq99Count++;
      logWarn("CSQ=99 detectado (" + String(csq99Count) + "x em sequência)");

      if (csq99Count >= 3) {
        recoverRadioFromUnknownSignal();
        csq99Count = 0;
        delay(1500);
      }
    } else {
      csq99Count = 0;
    }

    if (reg == 3) {
      logWarn("Registro negado. Forçando nova seleção automática de operadora.");
      forceAutomaticOperatorSelection();
    }

    delay(1500);
  }

  logErr("Falha no registro da rede móvel");
  return false;
}

// ------------------------------------------------------------
// DADOS MOVEIS
// ------------------------------------------------------------
bool connectGsmWithApn(const ApnConfig& cfg) {
  if (shouldPauseNonCriticalTasks()) {
    logWarn("Tentativa 4G pausada por prioridade MQTT");
    return false;
  }

  int simStatus = getSimStatusFromAT();
  if (simStatus != 3) {
    logWarn("Tentativa 4G abortada. SIM inválido: " + simStatusToString(simStatus));
    return false;
  }

  logStep("TENTATIVA 4G - " + String(cfg.operadora));

  logInfo("APN: " + String(cfg.apn));
  logInfo("Usuário APN: " + String(cfg.user));

  cleanNetworkStateBeforeApn();

  if (shouldPauseNonCriticalTasks()) {
    logWarn("Abertura de 4G pausada por prioridade MQTT");
    return false;
  }

  if (!waitForNetworkRegistration(45000UL)) {
    logErr("Registro de rede falhou antes da APN " + String(cfg.operadora));
    return false;
  }

  if (shouldPauseNonCriticalTasks()) {
    logWarn("Conexão GPRS pausada por prioridade MQTT");
    return false;
  }

  logStep("PRE-APN");
  disconnectCellularData();
  sendATExpectOk("AT+CGACT=0,1", 5000);
  reattachPacketDomain();
  sendATExpectOk("AT+CGATT?", 1500);
  sendATExpectOk("AT+CGACT?", 2000);

  logInfo("Abrindo contexto de dados móveis");
  bool gprsOk = modem.gprsConnect(cfg.apn, cfg.user, cfg.pass);

  if (!gprsOk) {
    logErr("Falha no gprsConnect() para " + String(cfg.operadora));
    logNetworkSnapshot();
    return false;
  }

  delay(3000);

  if (shouldPauseNonCriticalTasks()) {
    logWarn("Pós-conexão 4G pausada por prioridade MQTT");
    return false;
  }

  if (!modem.isGprsConnected()) {
    logErr("Dados móveis não ficaram ativos em " + String(cfg.operadora));
    logNetworkSnapshot();
    return false;
  }

  String ipStr = modem.getLocalIP();
  if (ipStr.length() == 0) ipStr = "IP não disponível";

  logOk("Conexão 4G ativa em " + String(cfg.operadora));
  logInfo("IP GSM: " + ipStr);
  sendATExpectOk("AT+CGATT?");
  sendATExpectOk("AT+CGACT?", 2000);
  sendATExpectOk("AT+CGPADDR", 2000);
  sendATExpectOk("AT+COPS?", 2500);

  usingGsm = true;
  usingWifi = false;
  resetSimPresenceDebounce();
  return true;
}

bool connectCellularWithFallback() {
  if (shouldPauseNonCriticalTasks()) {
    logWarn("Sequência 4G pausada por prioridade MQTT");
    return false;
  }

  int simStatus = getSimStatusFromAT();
  if (simStatus != 3) {
    logWarn("Sequência 4G abortada. SIM não pronto: " + simStatusToString(simStatus));
    return false;
  }

  logStep("SEQUENCIA DE OPERADORAS");

  for (int i = 0; i < APN_COUNT; i++) {
    if (shouldPauseNonCriticalTasks()) {
      logWarn("Troca de operadora pausada por prioridade MQTT");
      return false;
    }

    if (connectGsmWithApn(apns[i])) {
      return true;
    }
    logWarn("Falhou em " + String(apns[i].operadora) + ". Indo para a próxima opção.");
  }

  logErr("Todas as tentativas de 4G falharam");
  return false;
}

// ------------------------------------------------------------
// WIFI
// ------------------------------------------------------------
bool isWifiLinkHealthy() {
  wl_status_t st = WiFi.status();
  bool ok = (st == WL_CONNECTED);

  logInfo("Saúde Wi-Fi -> Status=" + String((int)st) +
          " | RSSI=" + String(WiFi.RSSI()) + " dBm");

  return ok;
}

void disconnectWifi() {
  if (WiFi.status() == WL_CONNECTED) {
    logInfo("Desconectando Wi-Fi");
  }
  WiFi.disconnect(true, false);
  usingWifi = false;
}

bool connectWifiFallback() {
  if (shouldPauseNonCriticalTasks()) {
    logWarn("Conexão Wi-Fi pausada por prioridade MQTT");
    return false;
  }

  logStep("FALLBACK WIFI");

  WiFi.mode(WIFI_STA);
  WiFi.begin(ssid, password);

  unsigned long start = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - start < 30000UL) {
    mqttLoopSafe();

    if (shouldPauseNonCriticalTasks()) {
      logWarn("Conexão Wi-Fi interrompida por prioridade MQTT");
      return false;
    }

    Serial.print(".");
    delay(500);
  }
  Serial.println();

  if (WiFi.status() != WL_CONNECTED) {
    logErr("Falha ao conectar no Wi-Fi");
    return false;
  }

  logOk("Wi-Fi conectado");
  logInfo("IP Wi-Fi: " + WiFi.localIP().toString());
  logInfo("RSSI Wi-Fi: " + String(WiFi.RSSI()) + " dBm");

  usingWifi = true;
  usingGsm = false;
  return true;
}

bool connectInternetWithFallback() {
  if (shouldPauseNonCriticalTasks()) {
    logWarn("Conectividade pausada por prioridade MQTT");
    return false;
  }

  logStep("CONECTIVIDADE");

  int simStatus = getSimStatusFromAT();
  if (simStatus == 3) {
    if (connectCellularWithFallback()) {
      logOk("Internet ativa via 4G");
      return true;
    }
  } else {
    logWarn("SIM não disponível no boot. Pulando 4G e indo para Wi-Fi.");
  }

  if (shouldPauseNonCriticalTasks()) {
    logWarn("Troca 4G -> Wi-Fi pausada por prioridade MQTT");
    return false;
  }

  logWarn("4G indisponível. Partindo para Wi-Fi.");

  if (connectWifiFallback()) {
    logOk("Internet ativa via Wi-Fi");
    return true;
  }

  logErr("Sem internet disponível por 4G e por Wi-Fi");
  return false;
}

// ------------------------------------------------------------
// CERTIFICADO NO MODEM
// ------------------------------------------------------------
bool modemHasCertificate(const String& filename) {
  String resp;
  if (!sendATExpectOk("AT+CCERTLIST", 10000, &resp)) {
    logWarn("AT+CCERTLIST falhou");
    return false;
  }

  return resp.indexOf(filename) >= 0;
}

bool uploadCertificateToModem(const String& filename, const char* pemData) {
  String certBody = String(pemData);
  int certLen = certBody.length();

  logStep("UPLOAD DO CERTIFICADO");
  logInfo("Arquivo: " + filename);
  logInfo("Tamanho: " + String(certLen) + " bytes");

  String cmd = "AT+CCERTDOWN=\"" + filename + "\"," + String(certLen);
  if (!sendATExpectPrompt(cmd, 15000)) {
    logErr("Falha ao abrir upload de certificado");
    return false;
  }

  String resp;
  SerialAT.write((const uint8_t*)certBody.c_str(), certLen);

  if (!waitForResponseContains(resp, 120000, "OK", "ERROR")) {
    logErr("Timeout no upload do certificado");
    return false;
  }

  String logResp = resp;
  logResp.replace("\r", " ");
  logResp.replace("\n", " | ");
  logInfo("CCERTDOWN => " + logResp);

  if (resp.indexOf("OK") >= 0) {
    logOk("Certificado enviado ao modem");
    return true;
  }

  logErr("Upload do certificado falhou");
  return false;
}

bool ensureRootCertificateInModem() {
  if (gsmCertReady) return true;

  if (modemHasCertificate(MODEM_CA_FILENAME)) {
    logOk("Root CA já existe no modem");
    gsmCertReady = true;
    return true;
  }

  if (!uploadCertificateToModem(MODEM_CA_FILENAME, root_ca)) {
    return false;
  }

  if (!modemHasCertificate(MODEM_CA_FILENAME)) {
    logErr("Certificado não apareceu em CCERTLIST após upload");
    return false;
  }

  gsmCertReady = true;
  logOk("Root CA pronta no modem");
  return true;
}

// ------------------------------------------------------------
// MQTT GSM NATIVO
// ------------------------------------------------------------
bool gsmMqttStop() {
  sendATExpectOk("AT+CMQTTDISC=0,120", 15000);
  sendATExpectOk("AT+CMQTTREL=0", 5000);
  sendATExpectOk("AT+CMQTTSTOP", 10000);

  gsmMqttConnected = false;
  gsmMqttClientAcquired = false;
  gsmMqttServiceStarted = false;
  return true;
}

bool gsmMqttConfigureTls() {
  logStep("CONFIGURACAO TLS DO MODEM");

  if (!ensureRootCertificateInModem()) {
    logErr("Root CA indisponível no modem");
    return false;
  }

  if (!sendATExpectOk("AT+CSSLCFG=\"sslversion\",0,4", 5000)) return false;
  if (!sendATExpectOk("AT+CSSLCFG=\"authmode\",0,1", 5000)) return false;
  if (!sendATExpectOk("AT+CSSLCFG=\"ignorelocaltime\",0,1", 5000)) return false;
  if (!sendATExpectOk("AT+CSSLCFG=\"enableSNI\",0,1", 5000)) return false;
  if (!sendATExpectOk("AT+CSSLCFG=\"cacert\",0,\"" + String(MODEM_CA_FILENAME) + "\"", 5000)) return false;

  logOk("Contexto TLS configurado");
  return true;
}

bool gsmMqttStartService() {
  logStep("START DO SERVICO MQTT(S)");

  sendATExpectOk("AT+CMQTTSTOP", 5000);

  String resp;
  if (!sendATExpectOk("AT+CMQTTSTART", 15000, &resp)) {
    if (resp.indexOf("+CMQTTSTART: 23") >= 0 || resp.indexOf("+CMQTTSTART: 0") >= 0) {
      logWarn("Serviço MQTT já estava ativo");
    } else {
      logErr("Falha em AT+CMQTTSTART");
      return false;
    }
  }

  gsmMqttServiceStarted = true;
  logOk("Serviço MQTT(S) ativo");
  return true;
}

bool gsmMqttAcquireClient(const String& clientId) {
  logStep("AQUISICAO DO CLIENTE MQTT");

  sendATExpectOk("AT+CMQTTREL=0", 3000);

  String cmd = "AT+CMQTTACCQ=0,\"" + clientId + "\",1";
  if (!sendATExpectOk(cmd, 8000)) {
    logErr("Falha em AT+CMQTTACCQ");
    return false;
  }

  if (!sendATExpectOk("AT+CMQTTSSLCFG=0,0", 5000)) {
    logErr("Falha em AT+CMQTTSSLCFG");
    return false;
  }

  gsmMqttClientAcquired = true;
  logOk("Cliente MQTT(S) adquirido");
  return true;
}

bool gsmMqttConnectBroker(const String& clientId) {
  logStep("CONEXAO MQTT(S) NO MODEM");

  if (!usingGsm) {
    logErr("Sem 4G ativo para MQTT(S)");
    return false;
  }

  if (!gsmMqttConfigureTls()) return false;
  if (!gsmMqttStartService()) return false;
  if (!gsmMqttAcquireClient(clientId)) return false;

  String serverAddr = "tcp://" + String(mqtt_server) + ":" + String(mqtt_server_port);
  String cmd = "AT+CMQTTCONNECT=0,\"" + serverAddr + "\",60,1,\"" + String(hiveIOTUser) + "\",\"" + String(hiveIOTPassword) + "\"";

  String resp;
  if (!sendATExpectOk(cmd, 30000, &resp)) {
    logErr("Falha ao enviar AT+CMQTTCONNECT");
    return false;
  }

  unsigned long start = millis();
  while (millis() - start < 30000) {
    while (SerialAT.available()) {
      char c = (char)SerialAT.read();
      resp += c;
    }

    if (resp.indexOf("+CMQTTCONNECT: 0,0") >= 0) {
      gsmMqttConnected = true;
      logOk("MQTT(S) conectado ao HiveMQ Cloud");
      return true;
    }

    if (resp.indexOf("+CMQTTCONNECT: 0,") >= 0 && resp.indexOf("+CMQTTCONNECT: 0,0") < 0) {
      String tmp = resp;
      tmp.replace("\r", " ");
      tmp.replace("\n", " | ");
      logErr("CMQTTCONNECT retornou erro: " + tmp);
      return false;
    }

    mqttLoopSafe();
    yield();
  }

  logErr("Timeout aguardando +CMQTTCONNECT");
  return false;
}

bool gsmMqttPublish(const String& topic, const String& payload, int qos = 1, int timeoutSec = 60, bool retained = false) {
  if (!gsmMqttConnected) {
    logErr("MQTT(S) GSM indisponível para publish");
    return false;
  }

  logInfo("Publicando tópico: " + topic);
  logInfo("Payload: " + payload);

  if (!sendATExpectPrompt("AT+CMQTTTOPIC=0," + String(topic.length()), 5000)) {
    logErr("Falha em AT+CMQTTTOPIC");
    return false;
  }
  if (!sendRawAfterPrompt(topic, 5000)) {
    logErr("Falha ao enviar topic");
    return false;
  }

  if (!sendATExpectPrompt("AT+CMQTTPAYLOAD=0," + String(payload.length()), 5000)) {
    logErr("Falha em AT+CMQTTPAYLOAD");
    return false;
  }
  if (!sendRawAfterPrompt(payload, 5000)) {
    logErr("Falha ao enviar payload");
    return false;
  }

  String resp;
  String cmd = "AT+CMQTTPUB=0," + String(qos) + "," + String(timeoutSec) + "," + String(retained ? 1 : 0);
  if (!sendATExpectOk(cmd, 10000, &resp)) {
    logErr("Falha ao publicar");
    return false;
  }

  unsigned long start = millis();
  while (millis() - start < 15000) {
    while (SerialAT.available()) {
      char c = (char)SerialAT.read();
      resp += c;
    }

    if (resp.indexOf("+CMQTTPUB: 0,0") >= 0) {
      logOk("Mensagem publicada no HiveMQ");
      return true;
    }

    if (resp.indexOf("+CMQTTPUB: 0,") >= 0 && resp.indexOf("+CMQTTPUB: 0,0") < 0) {
      String tmp = resp;
      tmp.replace("\r", " ");
      tmp.replace("\n", " | ");
      logErr("CMQTTPUB retornou erro: " + tmp);
      return false;
    }

    mqttLoopSafe();
    yield();
  }

  logErr("Timeout aguardando +CMQTTPUB");
  return false;
}

bool gsmMqttSubscribe(const String& topic, int qos = 1) {
  if (!gsmMqttConnected) {
    logErr("MQTT(S) GSM indisponível para subscribe");
    return false;
  }

  logInfo("Assinando tópico: " + topic);

  if (!sendATExpectPrompt("AT+CMQTTSUBTOPIC=0," + String(topic.length()) + "," + String(qos), 5000)) {
    logErr("Falha em AT+CMQTTSUBTOPIC");
    return false;
  }
  if (!sendRawAfterPrompt(topic, 5000)) {
    logErr("Falha ao enviar topic de subscribe");
    return false;
  }

  String resp;
  if (!sendATExpectOk("AT+CMQTTSUB=0", 10000, &resp)) {
    logErr("Falha ao executar subscribe");
    return false;
  }

  unsigned long start = millis();
  while (millis() - start < 15000) {
    while (SerialAT.available()) {
      char c = (char)SerialAT.read();
      resp += c;
    }

    if (resp.indexOf("+CMQTTSUB: 0,0") >= 0) {
      logOk("Subscribe realizado");
      return true;
    }

    if (resp.indexOf("+CMQTTSUB: 0,") >= 0 && resp.indexOf("+CMQTTSUB: 0,0") < 0) {
      String tmp = resp;
      tmp.replace("\r", " ");
      tmp.replace("\n", " | ");
      logErr("CMQTTSUB retornou erro: " + tmp);
      return false;
    }

    mqttLoopSafe();
    yield();
  }

  logErr("Timeout aguardando +CMQTTSUB");
  return false;
}

bool connectMqttGsmFull() {
  String clientId = "ESP32-A7670E-";
  clientId += String((uint32_t)ESP.getEfuseMac(), HEX);

  int attempts = 0;
  while (!gsmMqttConnected && attempts < 5) {
    attempts++;

    if (shouldPauseNonCriticalTasks()) {
      logWarn("Laço de reconexão MQTT GSM pausado por mensagem prioritária");
      return false;
    }

    logInfo("Tentativa MQTT(S) GSM " + String(attempts) + "/5");
    logInfo("ClientID: " + clientId);

    gsmMqttStop();

    if (gsmMqttConnectBroker(clientId)) {
      bool pubBoot = gsmMqttPublish(topicNameStablishConnection, messageOnceStablishConnection, 1, 60, false);
      bool subOk = gsmMqttSubscribe(topicBasicSensors, 1);

      if (pubBoot) {
        logOk("Mensagem inicial publicada");
      } else {
        logWarn("Falha ao publicar mensagem inicial");
      }

      if (subOk) {
        logOk("Inscrição no tópico realizada");
      } else {
        logWarn("Falha ao assinar tópico");
      }

      return true;
    }

    delay(1500);
  }

  logErr("Não foi possível conectar no MQTT(S) via GSM");
  return false;
}

// ------------------------------------------------------------
// MQTT WIFI
// ------------------------------------------------------------
bool connectMqttWifi() {
  if (shouldPauseNonCriticalTasks()) {
    logWarn("Reconexão MQTT Wi-Fi pausada por prioridade MQTT");
    return false;
  }

  logStep("CONEXAO MQTT WIFI");

  if (!usingWifi || WiFi.status() != WL_CONNECTED) {
    logErr("Wi-Fi indisponível para MQTT");
    return false;
  }

  String clientId = "ESP32-A7670E-";
  clientId += String((uint32_t)ESP.getEfuseMac(), HEX);

  int attempts = 0;
  while (!wifiMqttClient.connected() && attempts < 5) {
    attempts++;

    if (shouldPauseNonCriticalTasks()) {
      logWarn("Laço de reconexão MQTT Wi-Fi pausado por mensagem prioritária");
      return false;
    }

    logInfo("Tentativa MQTT Wi-Fi " + String(attempts) + "/5");
    logInfo("ClientID: " + clientId);

    if (wifiMqttClient.connect(clientId.c_str(), hiveIOTUser, hiveIOTPassword)) {
      logOk("MQTT Wi-Fi conectado com sucesso");

      if (wifiMqttClient.publish(topicNameStablishConnection, messageOnceStablishConnection)) {
        logOk("Mensagem inicial publicada em firstAttemptConnection");
      } else {
        logErr("Falha ao publicar mensagem inicial");
      }

      if (wifiMqttClient.subscribe(topicBasicSensors)) {
        logOk("Inscrição no tópico realizada");
      } else {
        logErr("Falha ao assinar o tópico");
      }

      return true;
    } else {
      logErr("Falha MQTT Wi-Fi. rc=" + String(wifiMqttClient.state()));
      delay(1500);
    }
  }

  logErr("Não foi possível conectar no MQTT via Wi-Fi");
  return false;
}

// ------------------------------------------------------------
// PREPARO DOS CLIENTES
// ------------------------------------------------------------
void prepareWifiMqttClient() {
  logStep("PREPARO DO CLIENTE MQTT WIFI");
  wifiClient.setCACert(root_ca);
  wifiMqttClient.setClient(wifiClient);
  wifiMqttClient.setServer(mqtt_server, mqtt_server_port);
  wifiMqttClient.setCallback(wifi_mqtt_callback);
  logInfo("Broker Wi-Fi: " + String(mqtt_server) + ":" + String(mqtt_server_port));
}

void prepareClients() {
  if (usingWifi) {
    prepareWifiMqttClient();
  } else if (usingGsm) {
    logStep("PREPARO DO CLIENTE MQTT GSM");
    logInfo("Broker GSM: " + String(mqtt_server) + ":" + String(mqtt_server_port));
  } else {
    logWarn("Nenhuma interface de rede ativa para o cliente MQTT");
  }
}

// ------------------------------------------------------------
// PUBLICACOES UNIFICADAS
// ------------------------------------------------------------
bool publishMessageUnified(const String& topic, const String& payload) {
  if (usingGsm && gsmMqttConnected) {
    return gsmMqttPublish(topic, payload, 1, 60, false);
  }

  if (usingWifi && wifiMqttClient.connected()) {
    payload.toCharArray(bufferMessage, MSG_BUFFER_SIZE);
    bool ok = wifiMqttClient.publish(topic.c_str(), bufferMessage);
    if (ok) logOk("Mensagem publicada via Wi-Fi");
    else logErr("Falha ao publicar via Wi-Fi");
    return ok;
  }

  logErr("Nenhum cliente MQTT disponível para publicação");
  return false;
}

bool publishAck(const String& command, const String& status, const String& detail) {
  String payload = "{";
  payload += "\"type\":\"ack\",";
  payload += "\"cmd\":\"" + command + "\",";
  payload += "\"status\":\"" + status + "\",";
  payload += "\"detail\":\"" + detail + "\",";
  payload += "\"network\":\"";
  payload += usingGsm ? "4G" : (usingWifi ? "WiFi" : "NONE");
  payload += "\",";
  payload += "\"millis\":" + String(millis());
  payload += "}";

  logInfo("ACK MQTT: " + payload);
  return publishMessageUnified(topicAck, payload);
}

// ------------------------------------------------------------
// RX URC MQTT GSM
// ------------------------------------------------------------
void processModemLine(const String& lineRaw) {
  String line = lineRaw;
  line.trim();
  if (!line.length()) return;

  if (gsmRx.expectingTopicData) {
    gsmRx.topic += line;
    gsmRx.expectingTopicData = false;
    return;
  }

  if (gsmRx.expectingPayloadData) {
    gsmRx.payload += line;
    gsmRx.expectingPayloadData = false;
    return;
  }

  if (line.startsWith("+CMQTTRXSTART:")) {
    gsmRx.receiving = true;
    gsmRx.topic = "";
    gsmRx.payload = "";
    gsmRx.topicTotalLen = 0;
    gsmRx.payloadTotalLen = 0;

    int c1 = line.indexOf(',');
    int c2 = line.indexOf(',', c1 + 1);
    if (c1 > 0 && c2 > c1) {
      gsmRx.topicTotalLen = line.substring(c1 + 1, c2).toInt();
      gsmRx.payloadTotalLen = line.substring(c2 + 1).toInt();
    }

    logInfo("Recebimento MQTT GSM iniciado. topicLen=" + String(gsmRx.topicTotalLen) +
            " payloadLen=" + String(gsmRx.payloadTotalLen));
    return;
  }

  if (line.startsWith("+CMQTTRXTOPIC:")) {
    gsmRx.expectingTopicData = true;
    return;
  }

  if (line.startsWith("+CMQTTRXPAYLOAD:")) {
    gsmRx.expectingPayloadData = true;
    return;
  }

  if (line.startsWith("+CMQTTRXEND:")) {
    logOk("Recebimento MQTT GSM concluído");
    logInfo("Tópico RX GSM: " + gsmRx.topic);
    logInfo("Payload RX GSM: " + gsmRx.payload);

    handleIncomingCommand(gsmRx.topic, gsmRx.payload);

    gsmRx.receiving = false;
    gsmRx.expectingTopicData = false;
    gsmRx.expectingPayloadData = false;
    gsmRx.topic = "";
    gsmRx.payload = "";
    return;
  }

  if (line.startsWith("+CMQTTDISC:")) {
    logWarn("Broker desconectou sessão MQTT GSM: " + line);
    gsmMqttConnected = false;
    return;
  }
}

void pollModemUrc() {
  static String lineBuf = "";

  while (SerialAT.available()) {
    char c = (char)SerialAT.read();

    if (c == '\r') continue;

    if (c == '\n') {
      if (lineBuf.length()) {
        processModemLine(lineBuf);
        lineBuf = "";
      }
      continue;
    }

    lineBuf += c;

    if (lineBuf.length() > 2048) {
      lineBuf = "";
    }
  }
}

// ------------------------------------------------------------
// MQTT RECONNECT UNIFICADO
// ------------------------------------------------------------
bool mqttConnectedUnified() {
  if (usingGsm) return gsmMqttConnected;
  if (usingWifi) return wifiMqttClient.connected();
  return false;
}

bool connectMqttUnified() {
  if (usingGsm) return connectMqttGsmFull();
  if (usingWifi) return connectMqttWifi();
  return false;
}

void stopMqttForInactiveNetwork() {
  if (usingGsm) {
    if (wifiMqttClient.connected()) {
      wifiMqttClient.disconnect();
    }
  } else if (usingWifi) {
    gsmMqttStop();
  }
}

void ensureMqtt() {
  if (!systemReady) return;
  if (mqttConnectedUnified()) return;
  if (shouldPauseNonCriticalTasks()) return;

  if (millis() - lastMqttReconnectAttempt < mqttReconnectInterval) return;
  lastMqttReconnectAttempt = millis();

  logWarn("MQTT desconectado. Tentando reconectar.");
  prepareClients();
  connectMqttUnified();
}

// ------------------------------------------------------------
// SUPERVISAO DE REDE
// ------------------------------------------------------------
bool switchToWifiWithLogs() {
  if (shouldPauseNonCriticalTasks()) {
    logWarn("Troca para Wi-Fi pausada por prioridade MQTT");
    return false;
  }

  logStep("TROCA PARA WIFI");

  gsmMqttStop();
  disconnectCellularData();

  if (connectWifiFallback()) {
    logOk("Troca para Wi-Fi concluída");
    prepareClients();
    connectMqttWifi();
    return true;
  }

  logErr("Falha na troca para Wi-Fi");
  return false;
}

bool switchToCellularWithLogs() {
  if (shouldPauseNonCriticalTasks()) {
    logWarn("Troca para 4G pausada por prioridade MQTT");
    return false;
  }

  logStep("TROCA PARA 4G");

  if (wifiMqttClient.connected()) {
    wifiMqttClient.disconnect();
  }
  disconnectWifi();

  if (connectCellularWithFallback()) {
    logOk("Troca para 4G concluída");
    prepareClients();
    connectMqttGsmFull();
    return true;
  }

  logErr("Falha na troca para 4G");
  return false;
}

void superviseNetwork() {
  if (!systemReady) return;
  if (shouldPauseNonCriticalTasks()) {
    logInfo("Supervisão de rede pausada por prioridade MQTT");
    return;
  }
  if (millis() - lastNetworkCheck < networkCheckInterval) return;

  lastNetworkCheck = millis();

  logStep("SUPERVISAO DE REDE");
  logInfo("Interface atual -> " + String(usingGsm ? "4G" : (usingWifi ? "Wi-Fi" : "NENHUMA")));

  if (usingGsm) {
    if (detectConfirmedSimRemoval()) {
      logErr("SIM removido detectado em runtime -> mudando para Wi-Fi");
      switchToWifiWithLogs();
      return;
    }

    bool healthy = isCellularLinkHealthy();

    if (healthy) {
      logOk("Link 4G saudável. Permanecendo no 4G.");
      return;
    }

    logWarn("Link 4G degradado ou perdido");

    if (millis() - lastCellularRecoveryAttempt >= cellularRecoveryInterval) {
      lastCellularRecoveryAttempt = millis();

      int simStatus = getSimStatusFromAT();
      if (simStatus != 3) {
        logWarn("4G não será recuperado agora porque o SIM não está pronto");
        if (switchToWifiWithLogs()) {
          logOk("Failover para Wi-Fi concluído após falha do SIM");
          return;
        }
        logErr("Nenhuma rede disponível após perda do SIM");
        return;
      }

      logWarn("Tentando recuperar a mesma conexão 4G");
      if (connectCellularWithFallback()) {
        logOk("4G recuperado");
        prepareClients();
        connectMqttGsmFull();
        return;
      }

      if (shouldPauseNonCriticalTasks()) return;

      logWarn("4G não voltou. Tentando Wi-Fi");
      if (switchToWifiWithLogs()) {
        logOk("Failover para Wi-Fi concluído");
        return;
      }

      logErr("Nenhuma rede disponível após queda do 4G");
    }

    return;
  }

  if (usingWifi) {
    bool healthy = isWifiLinkHealthy();

    if (healthy) {
      logOk("Link Wi-Fi saudável. Permanecendo no Wi-Fi.");

      if (millis() - lastCellularReturnAttempt >= cellularReturnInterval) {
        lastCellularReturnAttempt = millis();

        if (detectConfirmedSimReady()) {
          logInfo("SIM voltou enquanto o sistema está no Wi-Fi. Tentando retornar ao 4G.");

          if (switchToCellularWithLogs()) {
            logOk("Retorno automático ao 4G concluído");
            return;
          }

          logWarn("SIM voltou, mas o retorno ao 4G falhou. Permanecendo no Wi-Fi.");
        }
      }

      return;
    }

    logWarn("Link Wi-Fi degradado ou perdido");

    if (millis() - lastWifiRecoveryAttempt >= wifiRecoveryInterval) {
      lastWifiRecoveryAttempt = millis();

      logWarn("Tentando recuperar a mesma conexão Wi-Fi");
      if (connectWifiFallback()) {
        logOk("Wi-Fi recuperado");
        prepareClients();
        connectMqttWifi();
        return;
      }

      if (shouldPauseNonCriticalTasks()) return;

      int simStatus = getSimStatusFromAT();
      if (simStatus == 3) {
        logWarn("Wi-Fi não voltou. Tentando 4G");
        if (switchToCellularWithLogs()) {
          logOk("Failover para 4G concluído");
          return;
        }
      } else {
        logWarn("Wi-Fi caiu e o SIM não está disponível para 4G");
      }

      logErr("Nenhuma rede disponível após queda do Wi-Fi");
    }

    return;
  }

  logWarn("Nenhuma interface ativa. Tentando restabelecer conectividade");

  int simStatus = getSimStatusFromAT();
  if (simStatus == 3) {
    if (switchToCellularWithLogs()) {
      logOk("Conectividade restaurada via 4G");
      return;
    }
  } else {
    logWarn("Sem SIM pronto para tentar 4G neste momento");
  }

  if (switchToWifiWithLogs()) {
    logOk("Conectividade restaurada via Wi-Fi");
    return;
  }

  logErr("Ainda sem conectividade disponível");
}

// ------------------------------------------------------------
// GPS
// ------------------------------------------------------------
bool gpsPowerOff() {
  return sendATExpectOk("AT+CGNSSPWR=0", 1500);
}

bool gpsSetMode() {
  return sendATExpectOk("AT+CGNSSMODE=1", 1500);
}

bool gpsPowerOn() {
  gpsPowerOff();
  delay(500);

  gpsSetMode();
  delay(300);

  if (!sendATExpectOk("AT+CGNSSPWR=1", 2000)) {
    logErr("Sem resposta ao ligar GNSS");
    return false;
  }

  sendATExpectOk("AT+CGNSSPWR?", 1500);
  sendATExpectOk("AT+CGNSSMODE?", 1500);

  logOk("GNSS habilitado");
  return true;
}

bool readGps(double& lat, double& lon) {
  String resp;

  if (!sendATExpectOk("AT+CGNSSINFO", 2500, &resp)) {
    logErr("Sem resposta do GNSS");
    return false;
  }

  int idx = resp.indexOf("+CGNSSINFO:");
  if (idx < 0) {
    logErr("Resposta GNSS sem marcador +CGNSSINFO");
    return false;
  }

  int lineEnd = resp.indexOf('\n', idx);
  String line = (lineEnd > idx) ? resp.substring(idx, lineEnd) : resp.substring(idx);
  line.replace("\r", "");
  line.trim();

  String payload = line;
  payload.replace("+CGNSSINFO:", "");
  payload.trim();

  if (payload.length() == 0) {
    logWarn("GNSS sem payload útil");
    return false;
  }

  String fields[20];
  int fieldCount = 0;
  int start = 0;

  for (int i = 0; i <= payload.length(); i++) {
    if (i == payload.length() || payload.charAt(i) == ',') {
      if (fieldCount < 20) {
        fields[fieldCount++] = payload.substring(start, i);
      }
      start = i + 1;
    }
  }

  if (fieldCount < 9) {
    logErr("Campos GNSS insuficientes");
    return false;
  }

  String latRaw = fields[5];
  String latHem = fields[6];
  String lonRaw = fields[7];
  String lonHem = fields[8];

  if (latRaw.length() == 0 || latHem.length() == 0 || lonRaw.length() == 0 || lonHem.length() == 0) {
    logWarn("GNSS ligado, porém ainda sem coordenadas válidas");
    return false;
  }

  lat = latRaw.toDouble();
  lon = lonRaw.toDouble();

  if (latHem == "S") lat = -lat;
  if (latHem == "N") lat =  lat;

  if (lonHem == "W") lon = -lon;
  if (lonHem == "E") lon =  lon;

  if (lat == 0.0 && lon == 0.0) {
    logWarn("GNSS retornou coordenadas zeradas");
    return false;
  }

  return true;
}

bool publishGpsPayload(double lat, double lon) {
  String payload = "latitude: " + String(lat, 7) + ", longitude: " + String(lon, 7);
  logInfo("Payload GPS: " + payload);
  return publishMessageUnified(topicBasicSensors, payload);
}

void startGpsPublishCycle() {
  if (!systemReady) return;
  if (!mqttConnectedUnified()) return;
  if (gpsState != GPS_IDLE) return;
  if (shouldPauseNonCriticalTasks()) return;

  logStep("INICIO DO CICLO DE GPS");
  gpsAttemptCount = 0;
  gpsLat = 0.0;
  gpsLon = 0.0;
  gpsState = GPS_POWER_ON;
}

void processGpsPublisher() {
  if (!systemReady) return;
  if (gpsState == GPS_IDLE) return;

  if (shouldPauseNonCriticalTasks()) {
    logInfo("GPS pausado por prioridade MQTT");
    return;
  }

  switch (gpsState) {
    case GPS_POWER_ON:
      logInfo("Ativando GNSS para novo ciclo");

      if (!gpsPowerOn()) {
        logErr("Falha ao ativar GNSS neste ciclo");
        gpsState = GPS_IDLE;
        return;
      }

      logInfo("GNSS ligado. Aguardando janela inicial para primeiro fix");
      gpsNextReadAt = millis() + gpsInitialWaitMs;
      gpsState = GPS_WAIT_BEFORE_READ;
      return;

    case GPS_WAIT_BEFORE_READ:
      if (millis() >= gpsNextReadAt) {
        gpsState = GPS_READ_ATTEMPT;
      }
      return;

    case GPS_READ_ATTEMPT:
      gpsAttemptCount++;
      logInfo("Tentativa de leitura GPS " + String(gpsAttemptCount) + "/" + String(gpsMaxAttempts));

      if (readGps(gpsLat, gpsLon)) {
        logOk("GPS com coordenadas válidas");
        logInfo("Latitude: " + String(gpsLat, 7));
        logInfo("Longitude: " + String(gpsLon, 7));
        gpsState = GPS_PUBLISH;
        return;
      }

      if (gpsAttemptCount >= gpsMaxAttempts) {
        logErr("Não foi possível obter GPS após várias tentativas");
        gpsState = GPS_IDLE;
        return;
      }

      gpsNextReadAt = millis() + gpsRetryWaitMs;
      gpsState = GPS_WAIT_BEFORE_READ;
      return;

    case GPS_PUBLISH:
      publishGpsPayload(gpsLat, gpsLon);
      gpsState = GPS_IDLE;
      return;

    case GPS_IDLE:
    default:
      return;
  }
}

// ------------------------------------------------------------
// SETUP
// ------------------------------------------------------------
void setup() {
  Serial.begin(115200);
  delay(1000);

  logStep("BOOT DO SISTEMA");
  logInfo("Início do sistema");
  logInfo("Modelo da placa: " + String(PRODUCT_MODEL_NAME));

  setupPins();

  if (!initModem()) {
    logErr("Encerrando setup por falha no modem");
    return;
  }

  bool simReady = waitForSimReady(10000UL);

  if (!simReady) {
    logWarn("SIM não disponível no boot. Pulando 4G e indo direto para Wi-Fi.");

    if (!connectWifiFallback()) {
      logErr("Encerrando setup por falha no Wi-Fi sem SIM");
      return;
    }

    usingGsm = false;
    usingWifi = true;
  } else {
    logOk("SIM detectado. Seguindo fluxo normal 4G -> fallback Wi-Fi");
    logNetworkSnapshot();

    if (!connectInternetWithFallback()) {
      logErr("Encerrando setup por falta de internet");
      return;
    }
  }

  prepareClients();

  if (!connectMqttUnified()) {
    logErr("Encerrando setup por falha no MQTT");
    return;
  }

  systemReady = true;

  startGpsPublishCycle();
  lastGpsPublish = millis();

  logOk("Setup concluído");
}

// ------------------------------------------------------------
// LOOP
// ------------------------------------------------------------
void loop() {
  pollModemUrc();
  mqttLoopSafe();
  refreshMqttPriorityWindow();

  if (!systemReady) {
    delay(50);
    return;
  }

  if (shouldPauseNonCriticalTasks()) {
    logInfo("Janela de prioridade MQTT ativa. Demais processos aguardando.");
    mqttLoopSafe();
    delay(5);
    return;
  }

  superviseNetwork();
  mqttLoopSafe();

  ensureMqtt();
  mqttLoopSafe();

  processGpsPublisher();
  mqttLoopSafe();

  if (millis() - lastGpsPublish >= gpsInterval) {
    logInfo("Novo ciclo de publicação de GPS");
    startGpsPublishCycle();
    lastGpsPublish = millis();
  }

  delay(5);
}
