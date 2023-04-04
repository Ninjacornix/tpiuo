from confluent_kafka import Producer
import json, logging, time, requests

def main():
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)
    logging.info("Starting producer")
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    while True:
        r = requests.get(f'https://api.coincap.io/v2/assets/bitcoin/history?interval=h1').json()
        for asset in r['data']:
            producer.produce('prices', json.dumps(asset['priceUsd']).encode('utf-8'), callback=receipt_handler)
        producer.flush()
        time.sleep(1)


def receipt_handler(err, message):
    if err:
        logging.error("Message delivery failed: {}".format(err))
    else:
        logging.info("Message delivered to {} [{}]".format(message.topic(), message.partition()))

if __name__ == "__main__":
    main()