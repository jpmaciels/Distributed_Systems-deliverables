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