import dotenv from "dotenv"
import { mqtt } from 'aws-iot-device-sdk-v2';
import * as awscrt from "aws-crt";

const decoder = new TextDecoder('utf8');
const iot_lib = awscrt.iot;
const mqtt_lib = awscrt.mqtt;

async function execute_session(connection: mqtt.MqttClientConnection, options: any) {
    return new Promise<void>(async (resolve, reject) => {
        try {
            let subscribed = false;

            const on_publish = async (topic: string, payload: ArrayBuffer, dup: boolean, qos: mqtt.QoS, retain: boolean) => {
                const json = decoder.decode(payload);
                console.log(`Mensaje recibido. topic:"${topic}" dup:${dup} qos:${qos} retain:${retain}`);
                console.log(json);
                const message = JSON.parse(json);
                if (message.sequence == options.count) {
                    subscribed = true;
                    if (subscribed) {
                        resolve();
                    }
                }
            }

            await connection.subscribe(options.topic, mqtt.QoS.AtLeastOnce, on_publish);
        }
        catch (error) {
            reject(error);
        }
    });
}

const buildMQQTConnection = (options: any) => {

    let config_builder = iot_lib.AwsIotMqttConnectionConfigBuilder.new_mtls_builder_from_path(options.cert, options.key);

    if (options.ca_file != null) {
        config_builder.with_certificate_authority_from_path(undefined, options.ca_file);
    }

    config_builder.with_clean_session(true);
    config_builder.with_client_id(options.client_id || "test-" + Math.floor(Math.random() * 100000000));
    config_builder.with_endpoint(options.endpoint);
    const config = config_builder.build();

    const client = new mqtt_lib.MqttClient();
    return client.new_connection(config);
};

async function main() {

    // Leemos las configuraciones desde el archivo .env
    const options = {
        cert: process.env.CERT,
        key: process.env.KEY,
        ca_file: process.env.CA,
        endpoint: process.env.ENDPOINT,
        client_id: 'node',
        topic: 'prueba',
        count: 5,
        message: 'hola'
    }

    // Mostrar configuración actual

    console.log('Configuración actual');
    console.log(options);

    // Creamos una conexión MQTT con los certificados y llaves
    const connection = buildMQQTConnection(options);

    // Limitamos ejecución a 60 segundos
    const timer = setInterval(() => {
    }, 60 * 1000);

    // Conectamos al broker MQTT
    await connection.connect()
    // Ejecutamos la sesión con las configuraciones
    await execute_session(connection as any, options)
    // Desconectamos del brokert MQTT
    await connection.disconnect()

    // Permitimos a Node morir si la promesa se resuelve
    clearTimeout(timer);
}

dotenv.config()
main().then(r => console.log('Fin')).catch(error => console.log(error));
