import dotenv from "dotenv"
import { mqtt } from 'aws-iot-device-sdk-v2';
import * as awscrt from "aws-crt";

const iot_lib = awscrt.iot;
const mqtt_lib = awscrt.mqtt;

async function execute_session(connection: mqtt.MqttClientConnection, options: any) {
    return new Promise<void>(async (resolve, reject) => {
        try {
            let published = false;
            let published_counts = 0;
            // publicamos la cantidad de mensajes indicada en las configuraciones
            for (let seq = 0; seq < options.count; ++seq) {
                const publish = async () => {
                    const msg = {
                        message: options.message,
                        sequence: seq + 1,
                    };
                    const json = JSON.stringify(msg);

                    // Publicamos el mensaje y aumentamos el contador de secuencia
                    connection.publish(options.topic, json, mqtt.QoS.AtLeastOnce).then(() => {
                        console.log('Mensaje publicado');
                        console.log(msg);
                        ++published_counts;
                        if (published_counts == options.count) {
                            published = true;
                            if (published) {
                                resolve();
                            }
                        }
                    }).catch(err => console.log(err));
                }
                setTimeout(publish, seq * 1000);
            }
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

    config_builder.with_clean_session(false);
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
