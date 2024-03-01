<?php
/*
  Descrição do Desafio:
    Você precisa realizar uma migração dos dados fictícios que estão na pasta <dados_sistema_legado> para a base da clínica fictícia MedicalChallenge.
    Para isso, você precisa:
      1. Instalar o MariaDB na sua máquina. Dica: Você pode utilizar Docker para isso;
      2. Restaurar o banco da clínica fictícia Medical Challenge: arquivo <medical_challenge_schema>;
      3. Migrar os dados do sistema legado fictício que estão na pasta <dados_sistema_legado>:
        a) Dica: você pode criar uma função para importar os arquivos do formato CSV para uma tabela em um banco temporário no seu MariaDB.
      4. Gerar um dump dos dados já migrados para o banco da clínica fictícia Medical Challenge.
*/

// Importação de Bibliotecas:
include "./lib.php";

// Conexão com o banco da clínica fictícia:
$connMedical = mysqli_connect("localhost", "root", "root", "MedicalChallenge")
  or die("Não foi possível conectar os servidor MySQL: MedicalChallenge\n");

// Conexão com o banco temporário:
// $connTemp = mysqli_connect("localhost", "root", "root", "0temp")
//   or die("Não foi possível conectar os servidor MySQL: 0temp\n");

// Informações de Inicio da Migração:
echo "Início da Migração: " . dateNow() . ".\n\n";


// Iniciar transação
$connMedical->begin_transaction();

try{
    // Criar tabela temporária para agendamentos
    $connMedical->query( "CREATE TEMPORARY TABLE agendamentos_legado (
        cod_agendamento INT,
        descricao VARCHAR(255),
        dia DATE,
        hora_inicio TIME,
        hora_fim TIME,
        cod_paciente INT,
        paciente VARCHAR(255),
        cod_medico INT,
        medico VARCHAR(255),
        cod_convenio INT,
        convenio VARCHAR(255),
        procedimento VARCHAR(100)
    );");

    // Caminho  arquivo CSV
    $arquivo_csv = "20210512_agendamentos.csv";

    // Importar arquivo CSV para a tabela temporária
    $connMedical->query( "LOAD DATA INFILE '" . $arquivo_csv . "'
    INTO TABLE agendamentos_legado
    FIELDS TERMINATED BY ';'
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (cod_agendamento, descricao, @dia, hora_inicio, hora_fim, cod_paciente, paciente,
    cod_medico, medico, cod_convenio, convenio, @procedimento)
    SET dia = STR_TO_DATE(@dia, '%d/%m/%Y'),
    procedimento = TRIM(@procedimento_var);");


    // Criar tabela temporária para pacientes
    $connMedical->query("CREATE TEMPORARY TABLE pacientes_legado (
    cod_paciente INT,
    nome_paciente VARCHAR(255),
    nasc_paciente DATE,
    pai_paciente VARCHAR(255),
    mae_paciente VARCHAR(255),
    cpf_paciente VARCHAR(30),
    rg_paciente VARCHAR(30),
    sexo_pac CHAR(30),
    id_conv INT,
    convenio VARCHAR(255),
    obs_clinicas VARCHAR(255)
    );");

    // Caminho absoluto do arquivo CSV
    $arquivo_csv = "20210512_pacientes.csv";

    // Importar arquivo CSV para a tabela temporária
    $connMedical->query( "LOAD DATA INFILE '" . $arquivo_csv . "'
    INTO TABLE pacientes_legado
    FIELDS TERMINATED BY ';'
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (cod_paciente, nome_paciente, @nasc_paciente, pai_paciente, mae_paciente, cpf_paciente,
    rg_paciente, sexo_pac, id_conv, convenio, obs_clinicas)
    SET nasc_paciente = STR_TO_DATE(@nasc_paciente, '%d/%m/%Y'),
    obs_clinicas = TRIM(@obs_clinicas);");


    # MIGRAÇÃO DE DADOS

    # Migrar dados para a tabela convenios
    $connMedical->query("INSERT INTO CONVENIOS ( nome, descricao) 
    SELECT  distinct(convenio), obs_clinicas FROM pacientes_legado p 
    where p.convenio not in(select nome from CONVENIOS)
    ");
    # Ajustar dtabela temporaria pacientes_legado para id_conv
    $connMedical->query("UPDATE pacientes_legado p SET id_conv = (select id from  CONVENIOS where nome = p.convenio)");
    $connMedical->query("UPDATE agendamentos_legado p SET cod_convenio = (select id from  CONVENIOS where nome = p.convenio)");


    # Ajustar dtabela temporaria pacientes_legado para sexo
    $connMedical->query("UPDATE pacientes_legado p SET sexo_pac = CASE 
    WHEN sexo_pac = 'M' THEN 'Masculino'
    WHEN sexo_pac = 'F' THEN 'Feminino'
    ELSE sexo_pac
    END");
    # Migrar dados da tabela pacientes_legados para tabela pacientes
    $connMedical->query("INSERT INTO pacientes ( nome, sexo, nascimento, cpf, rg, id_convenio) 
    SELECT  nome_paciente,sexo_pac,nasc_paciente,cpf_paciente,rg_paciente,id_conv FROM pacientes_legado p
    where  p.cpf_paciente not in(select cpf from pacientes)");
    # Ajustar dtabela temporaria pacientes_legado para id_conv
    $connMedical->query("UPDATE pacientes_legado p SET cod_paciente = (select id from  pacientes where cpf = p.cpf_paciente)");
    $connMedical->query("UPDATE agendamentos_legado a SET cod_paciente = (select id from  pacientes where nome = a.paciente)");
   


    # Migrar dados para a tabela procedimentos 
    $connMedical->query("INSERT INTO procedimentos (nome) 
    SELECT  distinct(procedimento) FROM agendamentos_legado a where a.procedimento not in(select nome from procedimentos)
    ");
    # Ajustar dtabela temporaria agendamentos_legado para procedimento
    $connMedical->query("UPDATE agendamentos_legado a SET procedimento = (select nome from  procedimentos where nome = a.procedimento)");

    # Migrar dados para a tabela profissionais
    $connMedical->query("INSERT INTO profissionais ( nome) 
    SELECT  distinct(medico) FROM agendamentos_legado a where a.medico not in(select nome from profissionais)
    ");
    # Ajustar dtabela temporaria agendas para cod_medico
    $connMedical->query("UPDATE agendamentos_legado p SET cod_medico = (select id from  profissionais where nome = p.medico)");
 
    # Migrar dados para a tabela agendamentos   
    $connMedical->query("INSERT INTO agendamentos ( id_paciente, id_profissional, dh_inicio, dh_fim, id_convenio, id_procedimento) 
    SELECT (select id from pacientes where nome = a.paciente), 
    (select id from profissionais where nome = a.medico), 
    CONCAT(dia, ' ', hora_inicio),
    CONCAT(dia, ' ', hora_fim), 
    (select id from convenios where nome = a.convenio) , 
    (select id from procedimentos where trim(nome) = trim(a.procedimento))
    FROM agendamentos_legado a 
    ");

    # Excluir tabelas temporárias
    $connMedical->query("DROP TEMPORARY TABLE agendamentos_legado");
    $connMedical->query("DROP TEMPORARY TABLE pacientes_legado");

    $connMedical->commit();
  
} catch (Exception $e) {
    // Se ocorrer um erro, realizar rollback
    $connMedical->rollback();
    echo "Erro na transação: " . $e->getMessage();
}

// Encerrando as conexões:
$connMedical->close();
// $connTemp->close();

// Informações de Fim da Migração:
echo "Fim da Migração: " . dateNow() . ".\n";

