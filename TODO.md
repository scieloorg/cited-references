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


4. Verificar e corrigir campo 'tipo' em cada referência *DONE*

    i. 709 ou 71? Ou nenhum
    
    a. montar tabela descritiva que mostre os percentuais de cada tipo de referência

    b. verificar com Gustavo situação atual

    R. aparentemente será necessário coletar os dados em formato XMLRSPS, pois há certa inconsistência nos campos v709 e v71 do formato XYLOSE


5. Tratar com Gustavo

    a. script para obtenção de dados no portal issn

    b. dúvida sobre os campos 709 e 71 de cada referência
        
        i. no formato xylose, choices.py parece delinear os tipos de referências, mas o campo 71 é apontado como legacy e o campo 709 só apresenta dois tipos de referência (article | text)
        ii. no formato xmlrsps parece que o campo é bem preenchido
        iii. a informação correta está na base scielo; será possível corrigir o formato xylose da api article meta para trazer a informação corretamente ou será melhor trocar toda a obtenção dos dados para formato xmlrsps?

    c. dúvida sobre a integração (etapa iii do plano de trabalho)
    