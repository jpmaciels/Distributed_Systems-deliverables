"""
MS Lance (publisher e subscriber)
• Possui as chaves públicas de todos os clientes.
• (0,2) Escuta os eventos das filas lance_realizado, leilao_iniciado
e leilao_finalizado.
• (0,3) Recebe lances de usuários (ID do leilão; ID do usuário, valor
do lance) e checa a assinatura digital da mensagem utilizando a
chave pública correspondente. Somente aceitará o lance se:
o A assinatura for válida;
o ID do leilão existir e se o leilão estiver ativo;
o Se o lance for maior que o último lance registrado;
• (0,1) Se o lance for válido, o MS Lance publica o evento na fila
lance_validado.
• (0,2) Ao finalizar um leilão, deve publicar na fila leilao_vencedor,
informando o ID do leilão, o ID do vencedor do leilão e o valor
negociado. O vencedor é o que efetuou o maior lance válido até o
encerramento.
"""

import pika
import json
from loguru import logger
from typing import List
from Crypto.PublicKey import RSA
from Crypto.Signature import pkcs1_15
from Crypto.Hash import SHA256
import base64

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

class Auction:
    def __init__(self, auction_id, description, start_time, end_time, status):
        self.auction_id = auction_id
        self.description = description
        self.start_time = start_time
        self.end_time = end_time
        self.status = status
        self.bids = [] 
        self.highest_bid = 0.0
        self.highest_bidder = None


auctions: List[Auction] = []

def handle_auction_started(body):
    logger.info("Received auction started event")
    auction_data = json.loads(body)

    auction = Auction(
        auction_id=auction_data['id'],
        description=auction_data['description'],
        start_time=auction_data['start_time'],
        end_time=auction_data['end_time'],
        status=auction_data['status']
    )

    auctions.append(auction)
    logger.info(f"Auction created: {auction.auction_id} - {auction.description}")

def accept_bid(bid: dict, auction: Auction):
    auction.bids.append(bid)
    auction.highest_bid = bid['bid_amount']
    auction.highest_bidder = bid['user_id']
    logger.info(f"Bid accepted: Auction ID {auction.auction_id}, User ID {bid['user_id']}, Amount {bid['bid_amount']}")

def handle_bid_placed(body):
    logger.info("Received bid placed event")
    bid_data = json.loads(body)
    # body: {"auction_id": "123", "user_id": "456", "bid_amount": 100.0, "signature": "abc123"}

    # Only accepts if the signature is valid;
    # All clients public keys are stored in the 'keys' folder as {user_id}_public.pem

    try:
        with open(f"keys/{bid_data['user_id']}_public.pem", "rb") as f:
            public_key = RSA.import_key(f.read())
    except FileNotFoundError:
        logger.warning(f"Public key for user {bid_data['user_id']} not found. Bid rejected.")
        return
    except(ValueError, IndexError, TypeError) as e:
        logger.warning(f"Error loading public key for user {bid_data['user_id']}: {e}. Bid rejected.")
        return
    
    sig_b64 = bid_data.pop('signature')
    try:
        signature = base64.b64decode(sig_b64)
    except (base64.binascii.Error, ValueError) as e:
        logger.warning(f"Invalid base64 signature for user {bid_data['user_id']}: {e}. Bid rejected.")
        return
    
    signed_dict = {k: v for k, v in bid_data.items() if k != 'signature'}
    message = json.dumps(signed_dict, separators=(',', ':'), sort_keys=True).encode('utf-8')
    h = SHA256.new(message)

    try:
        pkcs1_15.new(public_key).verify(h, signature)
        logger.info(f"Signature verified for user {bid_data['user_id']}.")
    except (ValueError, TypeError):
        logger.warning(f"Invalid signature for user {bid_data['user_id']}. Bid rejected.")
        return

    if bid_data['auction_id'] not in [a.auction_id for a in auctions]:
        logger.warning(f"Auction ID {bid_data['auction_id']} does not exist. Client {bid_data['user_id']} bid rejected.")
        return
    
    auction = next(a for a in auctions if a.auction_id == bid_data['auction_id'])

    if auction.status != 'active':
        logger.warning(f"Auction ID {bid_data['auction_id']} is not active. Client {bid_data['user_id']} bid rejected.")
        return
    
    if bid_data['bid_amount'] <= auction.highest_bid:
        logger.warning(f"Bid amount {bid_data['bid_amount']} is not higher than current highest bid {auction.highest_bid}. Client {bid_data['user_id']} bid rejected.")
        return
    
    accept_bid(bid_data, auction)

    channel.basic_publish(
        exchange='direct_exchange',
        routing_key='bid_validated',
        body=json.dumps(bid_data)
    )


def handle_auction_ended(body):
    logger.info("Received auction ended event")
    auction_data = json.loads(body)
    # body: {"id": "123", "description": "Auction for item X", "start_time": "2023-10-01T10:00:00Z", "end_time": "2023-10-01T12:00:00Z", "status": "ended"}

    auction = next((a for a in auctions if a.auction_id == auction_data['id']), None)
    if not auction:
        logger.warning(f"Auction ID {auction_data['id']} not found.")
        return

    auction.status = 'ended'
    logger.info(f"Auction ended: {auction.auction_id}. Winner: {auction.highest_bidder} with bid {auction.highest_bid}")

    channel.basic_publish(
        exchange='direct_exchange',
        routing_key='auction_winner',
        body=json.dumps({
            "auction_id": auction.auction_id,
            "winner_user_id": auction.highest_bidder,
            "winning_bid_amount": auction.highest_bid
        })
    )

def callback(ch, method, properties, body):
    logger.info(f"Received in routing key {method.routing_key}: \n\t\t{body}")

    logger.debug(f"\n\nch: {ch}\nmethod: {method}\nproperties: {properties}\nbody: {body}\n\n")

    if method.exchange == 'auction_fanout_exchange':
        handle_auction_started(body)

    elif method.routing_key == 'bid_placed':
        handle_bid_placed(body)

    elif method.routing_key == 'auction_ended':
        handle_auction_ended(body)

def main():

    # Declare exchanges
    channel.exchange_declare(exchange='auction_fanout_exchange', exchange_type='fanout')
    channel.exchange_declare(exchange='direct_exchange', exchange_type='direct')

    # Declare queue
    channel.queue_declare(queue='ms_bid_queue')

    # bind queues to listen to
    channel.queue_bind(exchange='direct_exchange', queue='ms_bid_queue', routing_key='bid_placed')
    channel.queue_bind(exchange='direct_exchange', queue='ms_bid_queue', routing_key='auction_ended')
    channel.queue_bind(exchange='auction_fanout_exchange', queue='ms_bid_queue')

    channel.basic_consume(
        queue='ms_bid_queue', on_message_callback=callback, auto_ack=True)

    logger.info(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == "__main__":
    main()