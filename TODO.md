# To do list

1. Melhorar forma como atualização dos dicionários é realizada
    
    i. Trabalhar com parâmetro para localizar dicionários (arquivos .dat)
    
    a. Fazer backup de dicionários atuais (.dat)

    b. Atualizar dicionário atual com novos documentos

    c. Gerar novos dicionários (arquivos .dat)

    
2. Criar um script bash para automatizar todo o processo de execução de scripts semanais

    i. Acrescentar outros scripts, conforme forem criados

    a. Coletar pid

    b. Coletar document

    c. Atualizar references database

    d. Coletar metadata from crossref

    e. Atualizar metadata2pid


3. Melhorar forma como arquivo de coleta de novos pids é executado

    i. Parâmetro data deve apontar tanto a data de início quanto de fim

    a. Em vez de __new-pids-from-2019-07-27.csv__ deve ser __new-pids-from-2019-07-27-to-2019-08-13.csv__
