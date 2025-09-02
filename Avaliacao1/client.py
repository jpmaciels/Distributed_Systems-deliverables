"""
Cliente (publisher e subscriber)
• Não se comunica diretamente com nenhum serviço, toda a
comunicação é indireta através de filas de mensagens.
• (0,1) Logo ao inicializar, atuará como consumidor recebendo
eventos da fila leilao_iniciado. Os eventos recebidos contêm ID do
leilão, descrição, data e hora de início e fim.
• (0,2) Possui um par de chaves pública/privada. Publica lances na
fila de mensagens lance_realizado. Cada lance contém: ID do
leilão, ID do usuário, valor do lance. O cliente assina digitalmente
cada lance com sua chave privada.
• (0,2) Ao dar um lance em um leilão, o cliente atuará como
consumidor desse leilão, registrando interesse em receber
notificações quando um novo lance for efetuado no leilão de seu
interesse ou quando o leilão for encerrado. Por exemplo, se o
cliente der um lance no leilão de ID 1, ele escutará a fila leilao_1
"""