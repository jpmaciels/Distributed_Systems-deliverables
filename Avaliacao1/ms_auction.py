"""
MS Leilão (publisher)
• (0,1) Mantém internamente uma lista pré-configurada (hardcoded)
de leilões com: ID do leilão, descrição, data e hora de início e fim,
status (ativo, encerrado).
• (0,1) O leilão de um determinado produto deve ser iniciado quando
o tempo definido para esse leilão for atingido. Quando um leilão
começa, ele publica o evento na fila: leilao_iniciado.
• (0,1) O leilão de um determinado produto deve ser finalizado
quando o tempo definido para esse leilão expirar. Quando um leilão
termina, ele publica o evento na fila: leilao_finalizado.
"""

import pika
import json
import time
from datetime import datetime, timedelta
import threading

auctions = [
    {
        "id": 1,
        "description": "chocovo",
        "start_time": datetime.now() + timedelta(seconds=5),
        "end_time": datetime.now() + timedelta(seconds=15),
        "status": "active"
    },
    {
        "id": 2,
        "description": "chocopizza nachos",
        "start_time": datetime.now() + timedelta(seconds=10),
        "end_time": datetime.now() + timedelta(seconds=20),
        "status": "active"
    }
]

def connect_to_rabbitmq():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    return connection, channel

def publish_auction_event(auction, event_type):
    connection, channel = connect_to_rabbitmq()
    event = {
        "id": auction["id"],
        "description": auction["description"],
        "event_type": event_type,
        "timestamp": datetime.now().isoformat()
    }
    channel.basic_publish(
        exchange='',
        routing_key=f'auction_{event_type}',
        body=json.dumps(event)
    )
    print(f" [x] Published {event_type} event for auction {auction['id']}")
    connection.close()


def schedule_auction_events():
    for auction in auctions:
        start_delay = (auction["start_time"] - datetime.now()).total_seconds()
        if start_delay > 0:
            threading.Timer(start_delay, publish_auction_event, args=(auction, "active")).start()
        else:
            publish_auction_event(auction, "active")
        # Schedule auction end
        end_delay = (auction["end_time"] - datetime.now()).total_seconds()
        if end_delay > 0:
            threading.Timer(end_delay, publish_auction_event, args=(auction, "finished")).start()
        else:
            publish_auction_event(auction, "finished")

def main():
    schedule_auction_events()
    while True:
        time.sleep(1)

main()