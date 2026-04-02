/**
 * PROJETO PRINCIPAL - LILYGO TTGO T-Call A7670E V1.0
 *
 * Ajustes desta versão:
 *  - supervisão contínua de conectividade
 *  - failover 4G -> Wi-Fi
 *  - logs detalhados
 *  - MQTT tratado em todos os ciclos do loop
 *  - envio de GPS em máquina de estados, sem travar longos períodos
 *  - prioridade máxima para mensagem MQTT recebida
 *  - ACK MQTT ao receber e executar comando
 *  - pausa de GPS e reconexões após chegada de mensagem MQTT
 *  - uma vez conectada uma rede, não tenta outra enquanto a ativa estiver saudável
 */

#define TINY_GSM_RX_BUFFER 1024
#define TINY_GSM_MODEM_SIM7600

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
const char* topicAck = "rastreador";

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
  { "TIM",   "timbrasil.br",      "tim",   "tim"   },
  { "CLARO", "java.claro.com.br", "Claro", "Claro" },
  { "VIVO",  "zap.vivo.com.br",   "vivo",  "vivo"  }
};

const int APN_COUNT = sizeof(apns) / sizeof(apns[0]);

// ------------------------------------------------------------
// CLIENTES
// ------------------------------------------------------------
TinyGsmClient gsmClient(modem);
WiFiClientSecure wifiClient;
PubSubClient client;

// ------------------------------------------------------------
// ESTADO
// ------------------------------------------------------------
bool usingGsm = false;
bool usingWifi = false;
bool systemReady = false;

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

#define MSG_BUFFER_SIZE 512
char bufferMessage[MSG_BUFFER_SIZE];

// ------------------------------------------------------------
// PRIORIDADE MQTT
// ------------------------------------------------------------
volatile bool mqttPriorityActive = false;
volatile unsigned long mqttPriorityUntil = 0;
const unsigned long mqttPriorityHoldMs = 5000UL;

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
unsigned long gpsStateStartedAt = 0;
unsigned long gpsNextReadAt = 0;
int gpsAttemptCount = 0;
const int gpsMaxAttempts = 10;
double gpsLat = 0.0;
double gpsLon = 0.0;

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

void mqttLoopSafe() {
  if (systemReady && client.connected()) {
    client.loop();
  }
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

void serviceMqttPriorityPoint() {
  mqttLoopSafe();
  refreshMqttPriorityWindow();
}

bool publishAck(const String& command, const String& status, const String& detail) {
  if (!client.connected()) {
    logWarn("ACK não publicado pois MQTT está desconectado");
    return false;
  }

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

  payload.toCharArray(bufferMessage, MSG_BUFFER_SIZE);

  logInfo("ACK MQTT: " + payload);

  if (client.publish(topicAck, bufferMessage)) {
    logOk("ACK publicado em rastreador/ack");
    return true;
  }

  logErr("Falha ao publicar ACK");
  return false;
}

// ------------------------------------------------------------
// AUTOBAUD OFICIAL
// ------------------------------------------------------------
uint32_t AutoBaud()
{
  static uint32_t rates[] = {115200, 9600, 57600, 38400, 19200, 74400, 74880,
                             230400, 460800, 2400, 4800, 14400, 28800};

  for (uint8_t i = 0; i < sizeof(rates) / sizeof(rates[0]); i++) {
    uint32_t rate = rates[i];
    logInfo("Tentando baud rate " + String(rate));
    SerialAT.updateBaudRate(rate);
    delay(10);

    for (int j = 0; j < 10; j++) {
      while (SerialAT.available()) {
        SerialAT.read();
      }

      SerialAT.print("AT\r\n");
      String input = SerialAT.readString();

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
// AT
// ------------------------------------------------------------
void dumpModemOutput(uint32_t windowMs = 300) {
  unsigned long start = millis();
  String raw = "";

  while (millis() - start < windowMs) {
    while (SerialAT.available()) {
      char c = (char)SerialAT.read();
      raw += c;
    }
    serviceMqttPriorityPoint();
    yield();
  }

  raw.trim();
  if (raw.length() > 0) {
    raw.replace("\r", " ");
    raw.replace("\n", " | ");
    logInfo("URC/AT espontâneo => " + raw);
  }
}

bool sendATGetResponse(const String& cmd, String& response, uint32_t timeout = 1200) {
  response = "";

  while (SerialAT.available()) {
    SerialAT.read();
  }

  logInfo("Enviando AT: " + cmd);
  SerialAT.println(cmd);

  unsigned long start = millis();
  while (millis() - start < timeout) {
    while (SerialAT.available()) {
      char c = (char)SerialAT.read();
      response += c;
    }
    serviceMqttPriorityPoint();
    yield();
  }

  return response.length() > 0;
}

void logATCommand(const String& cmd, uint32_t timeout = 2000) {
  String resp;
  bool ok = sendATGetResponse(cmd, resp, timeout);

  if (!ok) {
    logWarn("Sem resposta para " + cmd);
    return;
  }

  String logResp = resp;
  logResp.replace("\r", " ");
  logResp.replace("\n", " | ");
  logInfo(cmd + " => " + logResp);
}

int getSimStatusFromAT() {
  String resp;

  if (!sendATGetResponse("AT+CPIN?", resp, 1200)) {
    logWarn("Falha ao consultar status do SIM por AT+CPIN?");
    return 0;
  }

  String upper = resp;
  upper.toUpperCase();

  String logResp = resp;
  logResp.replace("\r", " ");
  logResp.replace("\n", " | ");
  logInfo("Leitura SIM por AT => " + logResp);

  if (upper.indexOf("READY") >= 0) return 3;
  if (upper.indexOf("SIM REMOVED") >= 0) return 1;
  if (upper.indexOf("SIM PIN") >= 0 || upper.indexOf("PIN REQUIRED") >= 0) return 2;
  if (upper.indexOf("SIM PUK") >= 0 || upper.indexOf("PUK REQUIRED") >= 0) return 4;

  return 0;
}

void logATSnapshotCompleto() {
  logStep("SNAPSHOT AT COMPLETO");
  logATCommand("AT", 1200);
  logATCommand("ATI", 2000);
  logATCommand("AT+CPIN?", 1200);
  logATCommand("AT+CSQ", 1200);
  logATCommand("AT+CREG?", 1200);
  logATCommand("AT+CGREG?", 1200);
  logATCommand("AT+CEREG?", 1200);
  logATCommand("AT+COPS?", 2000);
  logATCommand("AT+CGATT?", 1200);
  logATCommand("AT+CGPADDR", 2000);
}

// ------------------------------------------------------------
// CALLBACK MQTT
// ------------------------------------------------------------
void mqtt_callback(char* topic, byte* payload, unsigned int length) {
  String commandArrived;

  Serial.print(nowTag() + "[MQTT] Mensagem recebida no tópico [");
  Serial.print(topic);
  Serial.print("]: ");

  for (unsigned int i = 0; i < length; i++) {
    commandArrived += (char)payload[i];
  }

  Serial.println(commandArrived);

  activateMqttPriority();
  logWarn("Mensagem MQTT recebeu prioridade máxima");

  bool commandRecognized = true;
  bool commandApplied = true;
  String detail = "executado";

  if (commandArrived == "ligarMotor") {
    digitalWrite(MOTORES, HIGH);
    logOk("Comando aplicado: motor ligado");
  } else if (commandArrived == "desligarMotor") {
    digitalWrite(MOTORES, LOW);
    logOk("Comando aplicado: motor desligado");
  } else if (commandArrived == "ligarLed") {
    digitalWrite(LED, LOW);
    logOk("Comando aplicado: LED externo ligado");
  } else if (commandArrived == "desligarLed") {
    digitalWrite(LED, HIGH);
    logOk("Comando aplicado: LED externo desligado");
  } else {
    commandRecognized = false;
    commandApplied = false;
    detail = "comando nao cadastrado";
    logWarn("Comando recebido, mas sem ação cadastrada");
  }

  if (commandRecognized && commandApplied) {
    publishAck(commandArrived, "ok", detail);
  } else {
    publishAck(commandArrived, "ignored", detail);
  }
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
// BOOT OFICIAL DO MODEM
// ------------------------------------------------------------
bool initModem() {
  logStep("INICIALIZACAO DO MODEM");

  SerialAT.begin(115200, SERIAL_8N1, MODEM_RX_PIN, MODEM_TX_PIN);
  logInfo("SerialAT iniciada");

#ifdef BOARD_POWERON_PIN
  logInfo("BOARD_POWERON_PIN acionado em HIGH");
#endif

  logInfo("Aplicando reset oficial do modem");
  digitalWrite(MODEM_RESET_PIN, !MODEM_RESET_LEVEL);
  delay(100);
  digitalWrite(MODEM_RESET_PIN, MODEM_RESET_LEVEL);
  delay(2600);
  digitalWrite(MODEM_RESET_PIN, !MODEM_RESET_LEVEL);

  logInfo("Aplicando pulso no PWRKEY");
  digitalWrite(BOARD_PWRKEY_PIN, LOW);
  delay(100);
  digitalWrite(BOARD_PWRKEY_PIN, HIGH);
  delay(100);
  digitalWrite(BOARD_PWRKEY_PIN, LOW);

  dumpModemOutput(1500);

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

  logATSnapshotCompleto();
  return true;
}

// ------------------------------------------------------------
// SIM
// ------------------------------------------------------------
bool waitForSimReady(unsigned long timeoutMs = 60000UL) {
  logStep("VERIFICACAO DO SIM");

  unsigned long start = millis();
  int lastStatus = -1;

  while (millis() - start < timeoutMs) {
    dumpModemOutput(80);

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

  logErr("Tempo esgotado aguardando SIM card");
  return false;
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
  int reg = modem.getRegistrationStatus();
  bool gprs = modem.isGprsConnected();

  logInfo("Saúde 4G -> Registro=" + regStatusToString(reg) +
          " | GPRS=" + String(gprs ? "CONECTADO" : "DESCONECTADO"));

  return (reg == 1 || reg == 5) && gprs;
}

bool waitForNetworkRegistration(unsigned long timeoutMs = 60000UL) {
  logStep("REGISTRO NA REDE MOVEL");

  unsigned long start = millis();
  unsigned long lastVerbose = 0;

  while (millis() - start < timeoutMs) {
    dumpModemOutput(80);

    if (shouldPauseNonCriticalTasks()) {
      logWarn("Registro na rede pausado por prioridade MQTT");
      return false;
    }

    int16_t csq = modem.getSignalQuality();
    int reg = modem.getRegistrationStatus();

    logInfo("Sinal CSQ=" + String(csq) + " (" + csqToDbmString(csq) + ")" +
            " | Registro=" + regStatusToString(reg));

    if (reg == 1 || reg == 5) {
      logOk("Registro na rede móvel concluído");
      logNetworkSnapshot();
      return true;
    }

    if (millis() - lastVerbose > 10000UL) {
      logNetworkSnapshot();
      lastVerbose = millis();
    }

    delay(1500);
  }

  logErr("Falha no registro da rede móvel");
  return false;
}

// ------------------------------------------------------------
// DADOS MOVEIS
// ------------------------------------------------------------
void disconnectCellularData() {
  logInfo("Desativando sessão de dados móveis anterior");
  modem.gprsDisconnect();
  delay(1000);
}

bool connectGsmWithApn(const ApnConfig& cfg) {
  if (shouldPauseNonCriticalTasks()) {
    logWarn("Tentativa 4G pausada por prioridade MQTT");
    return false;
  }

  logStep("TENTATIVA 4G - " + String(cfg.operadora));

  logInfo("APN: " + String(cfg.apn));
  logInfo("Usuário APN: " + String(cfg.user));

  disconnectCellularData();

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
  logATCommand("AT+CGATT?");
  logATCommand("AT+CGPADDR", 2000);

  usingGsm = true;
  usingWifi = false;
  return true;
}

bool connectCellularWithFallback() {
  if (shouldPauseNonCriticalTasks()) {
    logWarn("Sequência 4G pausada por prioridade MQTT");
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
    serviceMqttPriorityPoint();

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

  if (connectCellularWithFallback()) {
    logOk("Internet ativa via 4G");
    return true;
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
// MQTT
// ------------------------------------------------------------
void prepareClient() {
  logStep("PREPARO DO CLIENTE MQTT");

  if (usingGsm) {
    logInfo("Preparando cliente GSM para MQTT");
    client.setClient(gsmClient);
  } else if (usingWifi) {
    logInfo("Preparando cliente TLS sobre Wi-Fi");
    wifiClient.setCACert(root_ca);
    client.setClient(wifiClient);
  } else {
    logWarn("Nenhuma interface de rede ativa para o cliente MQTT");
  }

  client.setServer(mqtt_server, mqtt_server_port);
  client.setCallback(mqtt_callback);

  logInfo("Broker: " + String(mqtt_server) + ":" + String(mqtt_server_port));
}

bool connectMqtt() {
  if (shouldPauseNonCriticalTasks()) {
    logWarn("Reconexão MQTT pausada por prioridade MQTT");
    return false;
  }

  logStep("CONEXAO MQTT");

  if (!usingGsm && !usingWifi) {
    logErr("Sem rede ativa para MQTT");
    return false;
  }

  String clientId = "ESP32-A7670E-";
  clientId += String((uint32_t)ESP.getEfuseMac(), HEX);

  int attempts = 0;
  while (!client.connected() && attempts < 5) {
    attempts++;

    if (shouldPauseNonCriticalTasks()) {
      logWarn("Laço de reconexão MQTT pausado por mensagem prioritária");
      return false;
    }

    logInfo("Tentativa MQTT " + String(attempts) + "/5");
    logInfo("ClientID: " + clientId);

    if (client.connect(clientId.c_str(), hiveIOTUser, hiveIOTPassword)) {
      logOk("MQTT conectado com sucesso");

      if (client.publish(topicNameStablishConnection, messageOnceStablishConnection)) {
        logOk("Mensagem inicial publicada em firstAttemptConnection");
      } else {
        logErr("Falha ao publicar mensagem inicial");
      }

      if (client.subscribe(topicBasicSensors)) {
        logOk("Inscrição no tópico realizada");
      } else {
        logErr("Falha ao assinar o tópico");
      }

      return true;
    } else {
      logErr("Falha MQTT. rc=" + String(client.state()));
      delay(1500);
    }
  }

  logErr("Não foi possível conectar no MQTT");
  return false;
}

void ensureMqtt() {
  if (!systemReady) return;
  if (client.connected()) return;
  if (shouldPauseNonCriticalTasks()) return;

  if (millis() - lastMqttReconnectAttempt < mqttReconnectInterval) return;
  lastMqttReconnectAttempt = millis();

  logWarn("MQTT desconectado. Tentando reconectar.");

  prepareClient();
  connectMqtt();
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

  if (connectWifiFallback()) {
    logOk("Troca para Wi-Fi concluída");
    prepareClient();
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

  if (connectCellularWithFallback()) {
    logOk("Troca para 4G concluída");
    prepareClient();
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
    bool healthy = isCellularLinkHealthy();

    if (healthy) {
      logOk("Link 4G saudável. Permanecendo no 4G.");
      return;
    }

    logWarn("Link 4G degradado ou perdido");

    if (millis() - lastCellularRecoveryAttempt >= cellularRecoveryInterval) {
      lastCellularRecoveryAttempt = millis();

      logWarn("Tentando recuperar a mesma conexão 4G");
      if (switchToCellularWithLogs()) {
        logOk("4G recuperado");
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
      return;
    }

    logWarn("Link Wi-Fi degradado ou perdido");

    if (millis() - lastWifiRecoveryAttempt >= wifiRecoveryInterval) {
      lastWifiRecoveryAttempt = millis();

      logWarn("Tentando recuperar a mesma conexão Wi-Fi");
      if (connectWifiFallback()) {
        logOk("Wi-Fi recuperado");
        prepareClient();
        return;
      }

      if (shouldPauseNonCriticalTasks()) return;

      logWarn("Wi-Fi não voltou. Tentando 4G");
      if (switchToCellularWithLogs()) {
        logOk("Failover para 4G concluído");
        return;
      }

      logErr("Nenhuma rede disponível após queda do Wi-Fi");
    }

    return;
  }

  logWarn("Nenhuma interface ativa. Tentando restabelecer conectividade");

  if (switchToCellularWithLogs()) {
    logOk("Conectividade restaurada via 4G");
    return;
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
double convertNmeaToDecimal(const String& raw, char hemi) {
  if (raw.length() < 4) return 0.0;

  int dot = raw.indexOf('.');
  if (dot < 0) return 0.0;

  int degLen = (dot > 4) ? 3 : 2;
  double degrees = raw.substring(0, degLen).toDouble();
  double minutes = raw.substring(degLen).toDouble();

  double decimal = degrees + (minutes / 60.0);

  if (hemi == 'S' || hemi == 'W') decimal = -decimal;

  return decimal;
}

bool gpsPowerOn() {
  String resp;

  if (!sendATGetResponse("AT+CGNSSPWR=1", resp, 1200)) {
    logErr("Sem resposta ao ligar GNSS");
    return false;
  }

  resp.replace("\r", " ");
  resp.replace("\n", " | ");
  logInfo("Resposta GNSS: " + resp);

  if (resp.indexOf("OK") >= 0 || resp.indexOf("READY") >= 0) {
    logOk("GNSS habilitado");
    return true;
  }

  logWarn("Resposta inesperada ao ligar GNSS");
  return true;
}

bool readGps(double& lat, double& lon) {
  String resp;

  if (!sendATGetResponse("AT+CGNSSINFO", resp, 1200)) {
    logErr("Sem resposta do GNSS");
    return false;
  }

  String respLog = resp;
  respLog.replace("\r", " ");
  respLog.replace("\n", " | ");
  logInfo("CGNSSINFO: " + respLog);

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

  if (payload.length() == 0 || payload == ",,,,,,,," || payload.indexOf(",,,,") >= 0) {
    logWarn("GNSS ainda sem fix");
    return false;
  }

  String fields[20];
  int fieldCount = 0;
  int start = 0;

  for (int i = 0; i <= payload.length(); i++) {
    if (i == payload.length() || payload.charAt(i) == ',') {
      if (fieldCount < 20) fields[fieldCount++] = payload.substring(start, i);
      start = i + 1;
    }
  }

  if (fieldCount < 6) {
    logErr("Campos GNSS insuficientes");
    return false;
  }

  String latRaw = fields[2];
  String latHem = fields[3];
  String lonRaw = fields[4];
  String lonHem = fields[5];

  if (latRaw.length() == 0 || latHem.length() == 0 || lonRaw.length() == 0 || lonHem.length() == 0) {
    logWarn("GNSS sem latitude/longitude válidas");
    return false;
  }

  lat = convertNmeaToDecimal(latRaw, latHem.charAt(0));
  lon = convertNmeaToDecimal(lonRaw, lonHem.charAt(0));

  return true;
}

bool publishGpsPayload(double lat, double lon) {
  if (!client.connected()) {
    logErr("MQTT indisponível para envio do GPS");
    return false;
  }

  String payload = "{";
  payload += "\"type\":\"gps\",";
  payload += "\"lat\":" + String(lat, 6) + ",";
  payload += "\"lon\":" + String(lon, 6) + ",";
  payload += "\"source\":\"A7670E\",";
  payload += "\"network\":\"";
  payload += usingGsm ? "4G" : "WiFi";
  payload += "\"}";
  
  payload.toCharArray(bufferMessage, MSG_BUFFER_SIZE);

  logInfo("Payload GPS: " + payload);

  if (client.publish(topicBasicSensors, bufferMessage)) {
    logOk("Localização publicada no tópico");
    return true;
  }

  logErr("Falha ao publicar localização no tópico");
  return false;
}

void startGpsPublishCycle() {
  if (!systemReady) return;
  if (!client.connected()) return;
  if (gpsState != GPS_IDLE) return;
  if (shouldPauseNonCriticalTasks()) return;

  logStep("INICIO DO CICLO DE GPS");
  gpsAttemptCount = 0;
  gpsLat = 0.0;
  gpsLon = 0.0;
  gpsState = GPS_POWER_ON;
  gpsStateStartedAt = millis();
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
      gpsNextReadAt = millis() + 1500UL;
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
        logInfo("Latitude: " + String(gpsLat, 6));
        logInfo("Longitude: " + String(gpsLon, 6));
        gpsState = GPS_PUBLISH;
        return;
      }

      if (gpsAttemptCount >= gpsMaxAttempts) {
        logErr("Não foi possível obter fix GPS após várias tentativas");
        gpsState = GPS_IDLE;
        return;
      }

      gpsNextReadAt = millis() + 5000UL;
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

  if (!waitForSimReady(60000UL)) {
    logErr("Encerrando setup por falha no SIM");
    return;
  }

  logNetworkSnapshot();

  if (!connectInternetWithFallback()) {
    logErr("Encerrando setup por falta de internet");
    return;
  }

  prepareClient();

  if (!connectMqtt()) {
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
  dumpModemOutput(10);

  if (!systemReady) {
    delay(50);
    return;
  }

  mqttLoopSafe();
  refreshMqttPriorityWindow();

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
