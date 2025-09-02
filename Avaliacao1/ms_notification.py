"""
MS Notificação (publisher e subscriber)
• (0,2) Escuta os eventos das filas lance_validado e
leilao_vencedor.
• (0,2) Publica esses eventos nas filas específicas para cada leilão,
de acordo com o seu ID (leilao_1, leilao_2, ...), de modo que
somente os consumidores interessados nesses leilões recebam as
notificações correspondentes.
"""
