import pika
import json
import uuid
import threading
import base64
import os
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import serialization

# --- Settings Aligned with Microservices ---
RABBITMQ_HOST = 'localhost'
EXCHANGE_DIRECT = 'direct_exchange'
EXCHANGE_FANOUT = 'auction_fanout_exchange'
BID_PLACED_ROUTING_KEY = 'bid_placed'

class AuctionClient:
    def __init__(self):
        self.user_id = f"client_{uuid.uuid4().hex[:6]}"
        self.private_key, self.public_key = self._generate_or_load_keys()
        self._save_public_key()
        self.followed_auctions = set()
        print(f"Client '{self.user_id}' initialized.")

    def _generate_or_load_keys(self):
        """Generates a new RSA key pair or loads it if it already exists."""
        private_key_path = f"private_keys/{self.user_id}_private.pem"
        if os.path.exists(private_key_path):
            with open(private_key_path, "rb") as key_file:
                private_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=None
                )
        else:
            private_key = rsa.generate_private_key(
                public_exponent=65537,
                key_size=2048
            )
            with open(private_key_path, "wb") as f:
                f.write(private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                ))
        return private_key, private_key.public_key()

    def _save_public_key(self):
        """Saves the public key to a directory to be accessible by the Bid Microservice."""
        os.makedirs('public_keys', exist_ok=True)
        public_key_path = f"public_keys/{self.user_id}_public.pem"
        with open(public_key_path, "wb") as f:
            f.write(self.public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            ))
        print(f"Public key saved to: {public_key_path}")

    def _sign_message(self, message):
        """Signs a message (dictionary) with the private key."""
        message_bytes = json.dumps(message, sort_keys=True).encode('utf-8')
        signature = self.private_key.sign(
            message_bytes, padding.PKCS1v15(), hashes.SHA256()
        )
        return base64.b64encode(signature).decode('utf-8')

    def setup_messaging(self):
        """Configures the exchanges and starts the initial consumer thread."""
        # A configuração dos exchanges é feita na thread que for usá-los
        started_auctions_thread = threading.Thread(target=self._consume_started_auctions)
        started_auctions_thread.daemon = True
        started_auctions_thread.start()
        print("Listening for new auctions...")

    def _consume_started_auctions(self):
        """Consumes messages from the started auctions queue in its own thread."""
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        
        channel.exchange_declare(exchange=EXCHANGE_FANOUT, exchange_type='fanout')
        result = channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        channel.queue_bind(exchange=EXCHANGE_FANOUT, queue=queue_name)

        def callback(ch, method, properties, body):
            auction = json.loads(body)
            print("\n--- NEW AUCTION STARTED ---")
            print(f"  Auction ID: {auction.get('id')}")
            print(f"  Description: {auction.get('description')}")
            print(f"  End Time: {auction.get('end_time')}")
            print("---------------------------")
            ch.basic_ack(delivery_tag=method.delivery_tag)

        channel.basic_consume(queue=queue_name, on_message_callback=callback)
        channel.start_consuming()

    def place_bid(self, auction_id, value):
        """Publishes a bid for an auction."""
        bid_data = {
            'auction_id': auction_id,
            'user_id': self.user_id,
            'value': value
        }
        signature = self._sign_message(bid_data)
        full_message = { 'bid': bid_data, 'signature': signature }

        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        
        channel.exchange_declare(exchange=EXCHANGE_DIRECT, exchange_type='direct')
        
        channel.basic_publish(
            exchange=EXCHANGE_DIRECT,
            routing_key=BID_PLACED_ROUTING_KEY,
            body=json.dumps(full_message)
        )
        connection.close()
        
        print(f"Bid of ${value} sent for auction {auction_id}.")

        if auction_id not in self.followed_auctions:
            self._follow_auction(auction_id)

    def _follow_auction(self, auction_id):
        """Creates and consumes from a specific queue for an auction's notifications."""
        self.followed_auctions.add(auction_id)
        
        notifications_thread = threading.Thread(target=self._consume_notifications, args=(auction_id,))
        notifications_thread.daemon = True
        notifications_thread.start()
        
        routing_key = f"notifications_{auction_id}"
        print(f"Subscribed to receive notifications for auction {auction_id} (Routing Key: {routing_key}).")

    def _consume_notifications(self, auction_id):
        """Consumes messages from an auction-specific notification queue."""
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        
        channel.exchange_declare(exchange=EXCHANGE_DIRECT, exchange_type='direct')
        result = channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        
        routing_key = f"notifications_{auction_id}"
        channel.queue_bind(
            exchange=EXCHANGE_DIRECT,
            queue=queue_name,
            routing_key=routing_key
        )

        def callback(ch, method, properties, body):
            notification = json.loads(body)
            print(f"\n--- AUCTION NOTIFICATION {auction_id} ---")
            if 'vencedor' in notification:
                 print(f"  AUCTION CLOSED! Winner: {notification['vencedor']}, Amount: ${notification['valor']}")
            else:
                 print(f"  New bid registered by {notification.get('id_usuario')} for the amount of ${notification.get('valor')}")
            print("----------------------------------")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        channel.basic_consume(queue=queue_name, on_message_callback=callback)
        channel.start_consuming()

    def run(self):
        """Starts the client and the user interaction loop."""
        self.setup_messaging()
        while True:
            print("\nOptions: [1] Place bid | [exit] Exit")
            command = input("> ")
            if command == '1':
                try:
                    auction_id = input("  Auction ID: ")
                    value = float(input("  Bid amount: "))
                    self.place_bid(auction_id, value)
                except ValueError:
                    print("Invalid amount. Please try again.")
                except Exception as e:
                    print(f"An error occurred: {e}")
            elif command.lower() == 'exit':
                break
        # ALTERADO: Não há mais uma conexão principal para fechar
        # self.connection.close() # LINHA REMOVIDA
        print("Client shutdown.")

if __name__ == '__main__':
    client = AuctionClient()
    client.run()