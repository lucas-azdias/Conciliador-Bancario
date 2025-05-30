[ ] Implement Database class with logs
[ ] Go through all Exceptions and adjust naming (ValueError etc.)
[ ] Avaliar uso de AWS Lambda para executar código a partir do envio dos arquivos
[ ] How make it compile? Should I make rules? Could I use generalization with preset filter values?

[ ] FAZER UMA COLUNA PAYMENT_VALUE -> VALOR que será pago após correção por taxa válida (taxa válida será tabela com yield, type, start_time); end_time definido por haver outra com start_time maior
[ ] FAZER uma função map_type() para StatementEntry e para Finisher que usam dict str -> Type
[ ] Fazer uma função de inicialização init_types() em Type; tenta recuperar todos os types; caso não consiga todos 100%, então apagas todos e cria novos e depois executa validate_types() que reconstrói todas as ligações de tipos para Finisher, StatementEntry e Rate; isso deve ocorrer em sync_schema()