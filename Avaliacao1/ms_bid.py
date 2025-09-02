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