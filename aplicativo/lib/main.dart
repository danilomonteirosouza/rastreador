import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math' as math;

import 'package:flutter/material.dart';
import 'package:flutter_map/flutter_map.dart';
import 'package:latlong2/latlong.dart';
import 'package:mqtt_client/mqtt_client.dart';
import 'package:mqtt_client/mqtt_server_client.dart';
import 'package:url_launcher/url_launcher.dart';

void main() {
  runApp(const SubmarineApp());
}

class SubmarineApp extends StatelessWidget {
  const SubmarineApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Submarine Command',
      debugShowCheckedModeBanner: false,
      theme: ThemeData(
        brightness: Brightness.dark,
        primaryColor: const Color(0xFF64FFDA),
        scaffoldBackgroundColor: const Color(0xFF0A192F),
        colorScheme: const ColorScheme.dark(
          primary: Color(0xFF64FFDA),
          secondary: Color(0xFF112240),
        ),
      ),
      home: const MainCommandCenter(),
    );
  }
}

class MainCommandCenter extends StatefulWidget {
  const MainCommandCenter({super.key});

  @override
  State<MainCommandCenter> createState() => _MainCommandCenterState();
}

class _MainCommandCenterState extends State<MainCommandCenter> {
  final TextEditingController _urlController = TextEditingController(
    text: '231ccd91865148f78345c07e2d7e799e.s2.eu.hivemq.cloud',
  );
  final TextEditingController _userController = TextEditingController(
    text: 'rastreador',
  );
  final TextEditingController _passController = TextEditingController(
    text: 'Extranet1',
  );
  final TextEditingController _topicController = TextEditingController(
    text: 'rastreador',
  );

  final TextEditingController _radiusController = TextEditingController(
    text: '500',
  );
  final TextEditingController _phoneController = TextEditingController();

  MqttServerClient? client;
  StreamSubscription<List<MqttReceivedMessage<MqttMessage>>>?
  _updatesSubscription;

  bool isConnected = false;
  LatLng submarinePos = const LatLng(0, 0);
  LatLng? _areaCenter;
  String lastRawMessage = "Aguardando telemetria...";

  double _actingRadiusMeters = 500.0;
  String _notificationPhone = '';

  DateTime? _lastCoordinateAt;
  Timer? _offlineMonitorTimer;

  bool _outOfAreaAlertSent = false;
  bool _offlineAlertSent = false;

  static const Duration _offlineThreshold = Duration(seconds: 60);

  final MapController _mapController = MapController();
  final Distance _distance = const Distance();

  String _nowTag() {
    return "[${DateTime.now().toIso8601String()}]";
  }

  void _logInfo(String message) {
    print("${_nowTag()} [INFO] $message");
  }

  void _logWarn(String message) {
    print("${_nowTag()} [WARN] $message");
  }

  void _logError(String message) {
    print("${_nowTag()} [ERRO] $message");
  }

  void _logStep(String message) {
    print("");
    print("${_nowTag()} ==================== $message ====================");
  }

  Future<void> _connectMqtt() async {
    _logStep("INICIO DA CONEXAO MQTT");

    await _disconnectMqtt(showSnack: false);

    if (!mounted) return;
    setState(() => isConnected = false);

    _applySettingsFromFields();

    final host = _urlController.text.trim();
    final user = _userController.text.trim();
    final pass = _passController.text.trim();
    final topic = _topicController.text.trim();

    final clientId = user;

    _logInfo("Host: $host");
    _logInfo("Porta: 8883");
    _logInfo("Usuario: $user");
    _logInfo("Topico: $topic");
    _logInfo("ClientId: $clientId");
    _logInfo("Raio de atuacao: $_actingRadiusMeters m");
    _logInfo("Telefone notificacao: $_notificationPhone");

    client = MqttServerClient.withPort(host, clientId, 8883);

    client!.secure = true;
    client!.securityContext = SecurityContext.defaultContext;
    client!.keepAlivePeriod = 20;
    client!.connectTimeoutPeriod = 10000;
    client!.logging(on: true);

    client!.onDisconnected = _onDisconnected;
    client!.onConnected = _onConnected;
    client!.onSubscribed = _onSubscribed;

    client!.pongCallback = () {
      _logInfo("PONG recebido do broker");
    };

    try {
      _logInfo("Chamando client.connect(user, password)");
      final result = await client!.connect(user, pass);

      _logInfo(
        "Retorno do connect -> estado: ${result?.state} | "
            "codigo: ${result?.returnCode}",
      );
    } on Exception catch (e, s) {
      _logError("Excecao ao conectar no broker: $e");
      _logError("Stack trace: $s");
      _showSnackBar("Erro de conexão: $e");
      await _disconnectMqtt(showSnack: false);
      return;
    }

    _logInfo(
      "Status final apos connect -> "
          "state=${client?.connectionStatus?.state} | "
          "returnCode=${client?.connectionStatus?.returnCode}",
    );

    if (client?.connectionStatus?.state == MqttConnectionState.connected) {
      _logInfo("Cliente conectado com sucesso");
      setState(() => isConnected = true);

      _offlineAlertSent = false;
      _outOfAreaAlertSent = false;
      _subscribeToTopic(topic);
      _startOfflineMonitor();
    } else {
      _logError(
        "Falha na conexao MQTT. Status: ${client?.connectionStatus}",
      );
      _showSnackBar(
        "Falha ao conectar. Estado: ${client?.connectionStatus?.state} | "
            "Código: ${client?.connectionStatus?.returnCode}",
      );
      await _disconnectMqtt(showSnack: false);
    }
  }

  void _subscribeToTopic(String topicName) {
    _logStep("SUBSCRIBE MQTT");
    _logInfo("Inscrevendo no topico: $topicName");

    final result = client?.subscribe(topicName, MqttQos.atMostOnce);

    if (result == null) {
      _logWarn("Subscribe retornou null");
    } else {
      _logInfo("Subscribe enviado ao broker");
    }

    _updatesSubscription = client?.updates?.listen(
          (List<MqttReceivedMessage<MqttMessage>> messages) {
        _logInfo("Lote de mensagens recebido. Quantidade: ${messages.length}");

        if (messages.isEmpty) {
          _logWarn("Lote vazio recebido");
          return;
        }

        final received = messages.first;
        _logInfo("Topico da mensagem: ${received.topic}");

        final MqttPublishMessage recMess =
        received.payload as MqttPublishMessage;

        final String payload = MqttPublishPayload.bytesToStringAsString(
          recMess.payload.message,
        );

        _logInfo("Payload bruto recebido: $payload");

        if (!mounted) {
          _logWarn("Widget desmontado. Mensagem ignorada");
          return;
        }

        setState(() => lastRawMessage = payload);
        _processCoordinates(payload, received.topic);
      },
      onError: (error) {
        _logError("Erro no stream de mensagens MQTT: $error");
        _showSnackBar("Erro ao receber mensagens MQTT: $error");
      },
      onDone: () {
        _logWarn("Stream de mensagens MQTT encerrado");
      },
      cancelOnError: false,
    );
  }

  Future<void> _disconnectMqtt({bool showSnack = true}) async {
    _logStep("PROCESSO DE DESCONEXAO MQTT");

    _offlineMonitorTimer?.cancel();
    _offlineMonitorTimer = null;

    if (_updatesSubscription != null) {
      _logInfo("Cancelando stream de mensagens");
    }

    await _updatesSubscription?.cancel();
    _updatesSubscription = null;

    try {
      if (client != null) {
        _logInfo(
          "Desconectando cliente MQTT. Estado atual: "
              "${client?.connectionStatus?.state}",
        );
      }
      client?.disconnect();
    } catch (e) {
      _logError("Erro ao desconectar cliente MQTT: $e");
    }

    if (mounted) {
      setState(() {
        isConnected = false;
      });
    }

    _logInfo("Desconexao concluida");

    if (showSnack) {
      _showSnackBar("MQTT desconectado");
    }
  }

  void _onSubscribed(String topic) {
    _logStep("EVENTO MQTT SUBSCRIBED");
    _logInfo("Subscription confirmed for topic $topic");
    _showSnackBar("Inscrito no tópico: $topic");
  }

  void _onConnected() {
    _logStep("EVENTO MQTT CONNECTED");
    _logInfo(
      "Client connection was successful. "
          "Estado: ${client?.connectionStatus?.state} | "
          "codigo: ${client?.connectionStatus?.returnCode}",
    );

    if (!mounted) return;
    setState(() => isConnected = true);

    _offlineAlertSent = false;
    _startOfflineMonitor();

    _showSnackBar("Conectado ao broker com sucesso");
  }

  void _onDisconnected() {
    _logStep("EVENTO MQTT DISCONNECTED");
    _logWarn(
      "Client disconnection. "
          "Estado: ${client?.connectionStatus?.state} | "
          "codigo: ${client?.connectionStatus?.returnCode}",
    );

    _offlineMonitorTimer?.cancel();
    _offlineMonitorTimer = null;

    if (!mounted) return;
    setState(() => isConnected = false);

    _showSnackBar("MQTT desconectado");
  }

  void _processCoordinates(String message, String topic) {
    _logStep("PROCESSAMENTO DE COORDENADAS");
    _logInfo("Mensagem recebida para parsing: $message");

    try {
      final text = message.trim().toLowerCase();

      final RegExp regExp = RegExp(
        r'latitude:\s*([-+]?\d*\.?\d+)\s*,\s*longitude:\s*([-+]?\d*\.?\d+)',
      );

      final match = regExp.firstMatch(text);

      if (match != null) {
        final double lat = double.parse(match.group(1)!);
        final double lng = double.parse(match.group(2)!);

        _logInfo("Latitude extraida: $lat");
        _logInfo("Longitude extraida: $lng");

        _handleValidCoordinate(LatLng(lat, lng), topic);
        return;
      }

      _logWarn("Regex principal nao encontrou latitude/longitude");
      _processFallback(message, topic);
    } catch (e, s) {
      _logError("Erro no processamento das coordenadas: $e");
      _logError("Stack trace: $s");
      _showSnackBar("Erro no processamento das coordenadas: $e");
    }
  }

  void _processFallback(String message, String topic) {
    _logStep("FALLBACK DE PARSING");
    _logInfo("Tentando fallback com a mensagem: $message");

    try {
      final text = message.trim();

      if (text.startsWith('{')) {
        _logInfo("Mensagem detectada como JSON");
        final data = jsonDecode(text);
        final lat = (data['lat'] as num).toDouble();
        final lng = (data['lng'] as num).toDouble();

        _logInfo("Latitude JSON: $lat");
        _logInfo("Longitude JSON: $lng");

        _handleValidCoordinate(LatLng(lat, lng), topic);
        return;
      }

      final RegExp pureCoords = RegExp(
        r'^\s*([-+]?\d*\.?\d+)\s*,\s*([-+]?\d*\.?\d+)\s*$',
      );

      final match = pureCoords.firstMatch(text);
      if (match != null) {
        final lat = double.parse(match.group(1)!);
        final lng = double.parse(match.group(2)!);

        _logInfo("Latitude fallback simples: $lat");
        _logInfo("Longitude fallback simples: $lng");

        _handleValidCoordinate(LatLng(lat, lng), topic);
        return;
      }

      _logWarn("Fallback nao reconheceu o formato da mensagem");
    } catch (e, s) {
      _logError("Erro no fallback de parsing: $e");
      _logError("Stack trace: $s");
    }
  }

  void _handleValidCoordinate(LatLng pos, String topic) {
    _lastCoordinateAt = DateTime.now();
    _offlineAlertSent = false;

    if (_areaCenter == null) {
      _areaCenter = pos;
      _logInfo(
        "Centro da area de atuacao definido na primeira coordenada: "
            "${pos.latitude}, ${pos.longitude}",
      );
    }

    _updateMapPosition(pos);
    _checkAreaStatus(pos, topic);
  }

  void _updateMapPosition(LatLng pos) {
    _logStep("ATUALIZACAO DO MAPA");
    _logInfo(
      "Nova posicao do marcador: lat=${pos.latitude}, lng=${pos.longitude}",
    );

    if (!mounted) return;

    setState(() {
      submarinePos = pos;
    });

    _moveCamera(pos);
  }

  void _moveCamera(LatLng pos) {
    try {
      _logInfo(
        "Movendo mapa para lat=${pos.latitude}, lng=${pos.longitude}, zoom=15",
      );
      _mapController.move(pos, 15);
    } catch (e) {
      _logError("Falha ao mover o mapa: $e");
    }
  }

  void _startOfflineMonitor() {
    _offlineMonitorTimer?.cancel();

    _offlineMonitorTimer = Timer.periodic(const Duration(seconds: 5), (_) async {
      if (!isConnected) return;
      if (_lastCoordinateAt == null) return;

      final elapsed = DateTime.now().difference(_lastCoordinateAt!);

      if (elapsed >= _offlineThreshold && !_offlineAlertSent) {
        _offlineAlertSent = true;

        final topic = _topicController.text.trim();
        final lastLocationText = _formatLatLng(submarinePos);

        final message =
            "ALERTA: o tópico $topic está desconectado.\n"
            "Última localização recebida: $lastLocationText";

        _logWarn("Sem coordenadas novas por ${elapsed.inSeconds}s. Disparando alerta.");
        await _sendWhatsAppAlert(message);
      }
    });
  }

  void _checkAreaStatus(LatLng pos, String topic) {
    if (_areaCenter == null || _actingRadiusMeters <= 0) {
      _logWarn("Area de atuacao nao configurada para validacao");
      return;
    }

    final distanceMeters = _distance.as(LengthUnit.Meter, _areaCenter!, pos);

    _logInfo(
      "Distancia ate o centro da area: ${distanceMeters.toStringAsFixed(2)} m | "
          "Raio configurado: ${_actingRadiusMeters.toStringAsFixed(2)} m",
    );

    if (distanceMeters > _actingRadiusMeters) {
      if (!_outOfAreaAlertSent) {
        _outOfAreaAlertSent = true;

        final message =
            "ALERTA: o tópico $topic saiu da área de atuação.\n"
            "Localização atual: ${_formatLatLng(pos)}\n"
            "Centro da área: ${_formatLatLng(_areaCenter!)}\n"
            "Distância do centro: ${distanceMeters.toStringAsFixed(1)} m\n"
            "Raio configurado: ${_actingRadiusMeters.toStringAsFixed(1)} m";

        _sendWhatsAppAlert(message);
      }
    } else {
      if (_outOfAreaAlertSent) {
        _logInfo("Tópico voltou para dentro da área de atuação");
      }
      _outOfAreaAlertSent = false;
    }
  }

  Future<void> _sendWhatsAppAlert(String message) async {
    final phone = _sanitizePhone(_notificationPhone);

    if (phone.isEmpty) {
      _logWarn("Telefone de notificacao nao configurado");
      _showSnackBar("Defina o telefone de notificação para enviar alertas");
      return;
    }

    final encodedMessage = Uri.encodeComponent(message);
    final webUrl = Uri.parse("https://wa.me/$phone?text=$encodedMessage");

    _logStep("ENVIO DE ALERTA WHATSAPP");
    _logInfo("Telefone: $phone");
    _logInfo("Mensagem: $message");

    try {
      final ok = await launchUrl(
        webUrl,
        mode: LaunchMode.externalApplication,
      );

      if (!ok) {
        _logWarn("Nao foi possivel abrir o WhatsApp");
        _showSnackBar("Não foi possível abrir o WhatsApp");
      }
    } catch (e) {
      _logError("Erro ao abrir WhatsApp: $e");
      _showSnackBar("Erro ao abrir WhatsApp: $e");
    }
  }

  String _sanitizePhone(String phone) {
    return phone.replaceAll(RegExp(r'[^0-9]'), '');
  }

  String _formatLatLng(LatLng pos) {
    return "latitude: ${pos.latitude.toStringAsFixed(7)}, "
        "longitude: ${pos.longitude.toStringAsFixed(7)}";
  }

  void _applySettingsFromFields() {
    final parsedRadius = double.tryParse(
      _radiusController.text.trim().replaceAll(',', '.'),
    );

    _actingRadiusMeters = (parsedRadius != null && parsedRadius > 0)
        ? parsedRadius
        : 500.0;

    _notificationPhone = _phoneController.text.trim();

    if (mounted) {
      setState(() {});
    }
  }

  void _defineAreaCenterFromCurrentPosition() {
    _applySettingsFromFields();

    if (submarinePos.latitude == 0 && submarinePos.longitude == 0) {
      _showSnackBar("Ainda não há posição válida para definir o centro");
      return;
    }

    setState(() {
      _areaCenter = submarinePos;
      _outOfAreaAlertSent = false;
    });

    _logInfo(
      "Centro da area redefinido para posição atual: "
          "${submarinePos.latitude}, ${submarinePos.longitude}",
    );

    _showSnackBar("Centro da área definido com a posição atual");
  }

  void _showSnackBar(String msg) {
    _logInfo("SnackBar: $msg");

    if (!mounted) return;
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(content: Text(msg)),
    );
  }

  List<LatLng> _buildCirclePoints(
      LatLng center,
      double radiusMeters, {
        int pointsCount = 120,
      }) {
    final List<LatLng> points = [];

    final latRad = center.latitude * math.pi / 180.0;
    final metersPerDegreeLat = 111320.0;
    final metersPerDegreeLng = 111320.0 * math.cos(latRad);

    for (int i = 0; i <= pointsCount; i++) {
      final theta = (2 * math.pi * i) / pointsCount;

      final dx = radiusMeters * math.cos(theta);
      final dy = radiusMeters * math.sin(theta);

      final dLat = dy / metersPerDegreeLat;
      final dLng = dx / metersPerDegreeLng;

      points.add(
        LatLng(center.latitude + dLat, center.longitude + dLng),
      );
    }

    return points;
  }

  @override
  void dispose() {
    _logStep("DISPOSE DA TELA");
    _offlineMonitorTimer?.cancel();
    _disconnectMqtt(showSnack: false);
    _urlController.dispose();
    _userController.dispose();
    _passController.dispose();
    _topicController.dispose();
    _radiusController.dispose();
    _phoneController.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final circlePoints = (_areaCenter != null && _actingRadiusMeters > 0)
        ? _buildCirclePoints(_areaCenter!, _actingRadiusMeters)
        : <LatLng>[];

    return Scaffold(
      appBar: AppBar(
        title: const Text(
          'CENTRAL DE COMANDO',
          style: TextStyle(
            letterSpacing: 2,
            fontWeight: FontWeight.bold,
            fontSize: 16,
          ),
        ),
        centerTitle: true,
        backgroundColor: const Color(0xFF112240),
        actions: [
          IconButton(
            icon: Icon(
              Icons.settings,
              color: isConnected ? Colors.green : Colors.white,
            ),
            onPressed: _showSettingsSheet,
          ),
        ],
      ),
      body: Column(
        children: [
          Expanded(
            flex: 3,
            child: Container(
              margin: const EdgeInsets.all(15),
              decoration: BoxDecoration(
                border: Border.all(
                  color: const Color(0xFF64FFDA).withOpacity(0.3),
                  width: 2,
                ),
                borderRadius: BorderRadius.circular(25),
              ),
              child: ClipRRect(
                borderRadius: BorderRadius.circular(23),
                child: FlutterMap(
                  mapController: _mapController,
                  options: MapOptions(
                    initialCenter: submarinePos,
                    initialZoom: 2,
                  ),
                  children: [
                    TileLayer(
                      urlTemplate:
                      'https://tile.openstreetmap.org/{z}/{x}/{y}.png',
                      userAgentPackageName: 'com.example.rastreador',
                    ),
                    if (circlePoints.isNotEmpty)
                      PolygonLayer(
                        polygons: [
                          Polygon(
                            points: circlePoints,
                            color: Colors.cyan.withOpacity(0.15),
                            borderColor: Colors.cyanAccent,
                            borderStrokeWidth: 2,
                          ),
                        ],
                      ),
                    MarkerLayer(
                      markers: [
                        if (_areaCenter != null)
                          Marker(
                            point: _areaCenter!,
                            width: 60,
                            height: 60,
                            child: const Icon(
                              Icons.radio_button_checked,
                              size: 24,
                              color: Colors.yellow,
                            ),
                          ),
                        Marker(
                          point: submarinePos,
                          width: 80,
                          height: 80,
                          child: const Icon(
                            Icons.location_pin,
                            size: 50,
                            color: Colors.cyan,
                          ),
                        ),
                      ],
                    ),
                  ],
                ),
              ),
            ),
          ),
          Expanded(
            flex: 1,
            child: Container(
              width: double.infinity,
              padding: const EdgeInsets.symmetric(
                horizontal: 25,
                vertical: 20,
              ),
              decoration: const BoxDecoration(
                color: Color(0xFF112240),
                borderRadius: BorderRadius.only(
                  topLeft: Radius.circular(40),
                  topRight: Radius.circular(40),
                ),
                boxShadow: [
                  BoxShadow(
                    color: Colors.black54,
                    blurRadius: 10,
                    offset: Offset(0, -5),
                  ),
                ],
              ),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Row(
                    mainAxisAlignment: MainAxisAlignment.spaceBetween,
                    children: [
                      const Text(
                        'DADOS DO RASTREADOR',
                        style: TextStyle(
                          color: Color(0xFF64FFDA),
                          fontSize: 12,
                          fontWeight: FontWeight.bold,
                          letterSpacing: 1.5,
                        ),
                      ),
                      Container(
                        padding: const EdgeInsets.symmetric(
                          horizontal: 10,
                          vertical: 4,
                        ),
                        decoration: BoxDecoration(
                          color: isConnected
                              ? Colors.green.withOpacity(0.1)
                              : Colors.red.withOpacity(0.1),
                          borderRadius: BorderRadius.circular(10),
                        ),
                        child: Text(
                          isConnected ? 'ONLINE' : 'OFFLINE',
                          style: TextStyle(
                            color: isConnected ? Colors.green : Colors.red,
                            fontSize: 10,
                            fontWeight: FontWeight.bold,
                          ),
                        ),
                      ),
                    ],
                  ),
                  const SizedBox(height: 14),
                  Row(
                    children: [
                      _buildCoordItem(
                        "LATITUDE",
                        submarinePos.latitude.toStringAsFixed(7),
                      ),
                      const SizedBox(width: 28),
                      _buildCoordItem(
                        "LONGITUDE",
                        submarinePos.longitude.toStringAsFixed(7),
                      ),
                    ],
                  ),
                  const SizedBox(height: 10),
                  Text(
                    'RAIO: ${_actingRadiusMeters.toStringAsFixed(0)} m',
                    style: const TextStyle(
                      color: Colors.white70,
                      fontSize: 12,
                      fontWeight: FontWeight.w600,
                    ),
                  ),
                  Text(
                    _areaCenter != null
                        ? 'CENTRO: ${_areaCenter!.latitude.toStringAsFixed(5)}, ${_areaCenter!.longitude.toStringAsFixed(5)}'
                        : 'CENTRO: não definido',
                    style: const TextStyle(
                      color: Colors.white54,
                      fontSize: 11,
                    ),
                  ),
                  const Spacer(),
                  Text(
                    'MENSAGEM BRUTA: $lastRawMessage',
                    style: const TextStyle(
                      color: Colors.white24,
                      fontSize: 9,
                      fontStyle: FontStyle.italic,
                    ),
                  ),
                ],
              ),
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildCoordItem(String label, String value) {
    return Column(
      crossAxisAlignment: CrossAxisAlignment.start,
      children: [
        Text(
          label,
          style: const TextStyle(
            color: Colors.white38,
            fontSize: 10,
            fontWeight: FontWeight.bold,
          ),
        ),
        Text(
          value,
          style: const TextStyle(
            fontFamily: 'monospace',
            fontSize: 22,
            color: Colors.white,
          ),
        ),
      ],
    );
  }

  void _showSettingsSheet() {
    showModalBottomSheet(
      context: context,
      isScrollControlled: true,
      backgroundColor: const Color(0xFF0A192F),
      shape: const RoundedRectangleBorder(
        borderRadius: BorderRadius.vertical(top: Radius.circular(35)),
      ),
      builder: (context) => Padding(
        padding: EdgeInsets.only(
          bottom: MediaQuery.of(context).viewInsets.bottom,
          left: 30,
          right: 30,
          top: 30,
        ),
        child: SingleChildScrollView(
          child: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              Container(
                width: 50,
                height: 5,
                decoration: BoxDecoration(
                  color: Colors.white12,
                  borderRadius: BorderRadius.circular(10),
                ),
              ),
              const SizedBox(height: 25),
              const Text(
                'CONFIGURAR BROKER',
                style: TextStyle(
                  fontSize: 18,
                  fontWeight: FontWeight.bold,
                  color: Color(0xFF64FFDA),
                ),
              ),
              const SizedBox(height: 25),
              _buildField(_urlController, 'Endereço HiveMQ', Icons.dns),
              _buildField(_userController, 'Usuário', Icons.account_circle),
              _buildField(_passController, 'Senha', Icons.vpn_key, isPass: true),
              _buildField(_topicController, 'Tópico de Escuta', Icons.sensors),
              _buildField(
                _radiusController,
                'Raio de atuação (metros)',
                Icons.radar,
              ),
              _buildField(
                _phoneController,
                'Telefone de notificação (com DDI)',
                Icons.phone_android,
              ),
              const SizedBox(height: 8),
              ElevatedButton.icon(
                style: ElevatedButton.styleFrom(
                  backgroundColor: Colors.orange,
                  minimumSize: const Size(double.infinity, 54),
                  shape: RoundedRectangleBorder(
                    borderRadius: BorderRadius.circular(18),
                  ),
                ),
                onPressed: () {
                  Navigator.pop(context);
                  _defineAreaCenterFromCurrentPosition();
                },
                icon: const Icon(Icons.my_location, color: Colors.black),
                label: const Text(
                  'USAR POSIÇÃO ATUAL COMO CENTRO',
                  style: TextStyle(
                    color: Colors.black,
                    fontWeight: FontWeight.bold,
                  ),
                ),
              ),
              const SizedBox(height: 18),
              ElevatedButton(
                style: ElevatedButton.styleFrom(
                  backgroundColor: const Color(0xFF64FFDA),
                  minimumSize: const Size(double.infinity, 60),
                  shape: RoundedRectangleBorder(
                    borderRadius: BorderRadius.circular(18),
                  ),
                  elevation: 5,
                ),
                onPressed: isConnected
                    ? null
                    : () {
                  _applySettingsFromFields();
                  Navigator.pop(context);
                  _connectMqtt();
                },
                child: Text(
                  isConnected ? 'CONECTADO' : 'CONECTAR SISTEMA',
                  style: const TextStyle(
                    color: Color(0xFF0A192F),
                    fontWeight: FontWeight.bold,
                    fontSize: 16,
                  ),
                ),
              ),
              const SizedBox(height: 14),
              ElevatedButton(
                style: ElevatedButton.styleFrom(
                  backgroundColor: Colors.red,
                  disabledBackgroundColor: Colors.red.withOpacity(0.35),
                  minimumSize: const Size(double.infinity, 60),
                  shape: RoundedRectangleBorder(
                    borderRadius: BorderRadius.circular(18),
                  ),
                  elevation: 5,
                ),
                onPressed: isConnected
                    ? () async {
                  Navigator.pop(context);
                  await _disconnectMqtt();
                }
                    : null,
                child: const Text(
                  'DESCONECTAR',
                  style: TextStyle(
                    color: Colors.white,
                    fontWeight: FontWeight.bold,
                    fontSize: 16,
                  ),
                ),
              ),
              const SizedBox(height: 40),
            ],
          ),
        ),
      ),
    );
  }

  Widget _buildField(
      TextEditingController controller,
      String label,
      IconData icon, {
        bool isPass = false,
      }) {
    return Padding(
      padding: const EdgeInsets.only(bottom: 18),
      child: TextField(
        controller: controller,
        obscureText: isPass,
        keyboardType: isPass ? TextInputType.text : TextInputType.text,
        style: const TextStyle(color: Colors.white),
        decoration: InputDecoration(
          labelText: label,
          labelStyle: const TextStyle(
            color: Colors.white38,
            fontSize: 13,
          ),
          prefixIcon: Icon(
            icon,
            color: const Color(0xFF64FFDA),
            size: 20,
          ),
          filled: true,
          fillColor: const Color(0xFF112240),
          border: OutlineInputBorder(
            borderRadius: BorderRadius.circular(18),
            borderSide: BorderSide.none,
          ),
          focusedBorder: OutlineInputBorder(
            borderRadius: BorderRadius.circular(18),
            borderSide: const BorderSide(
              color: Color(0xFF64FFDA),
              width: 1,
            ),
          ),
        ),
      ),
    );
  }
}