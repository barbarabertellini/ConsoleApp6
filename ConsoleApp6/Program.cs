using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.V2;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BigQueryConnector
{
    public class BigQueryConnection
    {
        private BigQueryClient _client;

        // ======== CONFIGURAÇÕES PRINCIPAIS ========
        private readonly string _projectId = "pim-4-468401"; // <-- seu Project ID
        private readonly string _jsonCredentialsPath = @"C:\Users\barba\Downloads\pim-4-468401-40a628367b22.json"; // <-- caminho do JSON
        private readonly string _datasetId = "pim_suporte"; // <-- dataset que contem suas tabelas

        // ======== CONEXÃO ========
        public void Connect()
        {
            var credential = GoogleCredential.FromFile(_jsonCredentialsPath);
            _client = BigQueryClient.Create(_projectId, credential);
            Console.WriteLine("✅ Conexão com BigQuery estabelecida!");
        }

        // ======== CONSULTAS ========
        // Exemplo: ler mensagens do Chat
        public async Task<List<Dictionary<string, object>>> GetMensagensAsync()
        {
            string sql = $"SELECT * FROM `{_projectId}.{_datasetId}.Chat` ORDER BY Data_Hora_Envio DESC";
            var result = await _client.ExecuteQueryAsync(sql, parameters: null);

            var rows = new List<Dictionary<string, object>>();
            foreach (var row in result)
            {
                var rowData = new Dictionary<string, object>();
                foreach (var field in row.Schema.Fields)
                    rowData[field.Name] = row[field.Name];
                rows.Add(rowData);
            }

            return rows;
        }

        // Exemplo: consultar todas as pessoas
        public async Task<List<Dictionary<string, object>>> GetPessoasAsync()
        {
            string sql = $"SELECT ID_Pessoa, Nome_Pessoa, Email, Cargo, Status FROM `{_projectId}.{_datasetId}.Pessoas`";
            var result = await _client.ExecuteQueryAsync(sql, parameters: null);

            var rows = new List<Dictionary<string, object>>();
            foreach (var row in result)
            {
                var rowData = new Dictionary<string, object>();
                foreach (var field in row.Schema.Fields)
                    rowData[field.Name] = row[field.Name];
                rows.Add(rowData);
            }

            return rows;
        }

        // Exemplo: listar todos os tickets abertos
        public async Task<List<Dictionary<string, object>>> GetTicketsAbertosAsync()
        {
            string sql = $"SELECT * FROM `{_projectId}.{_datasetId}.Ticket` WHERE Status_Ticket = 'Aberto'";
            var result = await _client.ExecuteQueryAsync(sql, parameters: null);

            var rows = new List<Dictionary<string, object>>();
            foreach (var row in result)
            {
                var rowData = new Dictionary<string, object>();
                foreach (var field in row.Schema.Fields)
                    rowData[field.Name] = row[field.Name];
                rows.Add(rowData);
            }

            return rows;
        }

        // ======== INSERÇÕES ========

        // Inserir uma nova mensagem no Chat
        public async Task InserirMensagemAsync(int idChat, int idRemetente, int idDestinatario, string mensagem)
        {
            string tableId = "Chat";
            var table = _client.GetTable(_datasetId, tableId);

            // Busca o último ID_Mensagem existente
            string sql = $"SELECT MAX(id_mensagem) AS max_id FROM `{_projectId}.{_datasetId}.{tableId}`";
            var result = await _client.ExecuteQueryAsync(sql, parameters: null);
            long novoId = 1; // caso a tabela esteja vazia

            foreach (var row in result)
            {
                if (row["max_id"] != DBNull.Value && row["max_id"] != null)
                    novoId = Convert.ToInt64(row["max_id"]) + 1;
            }

            // Formata a data/hora no padrão DATETIME do BigQuery (sem 'Z')
            string dataHoraEnvio = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);

            var insertRow = new BigQueryInsertRow
            {
                { "id_mensagem", novoId },
                { "id_chat", idChat },
                { "id_remetente", idRemetente },
                { "id_destinatario", idDestinatario },
                { "mensagem", mensagem },
                { "data_hora_envio", DateTime.UtcNow }
            };

            BigQueryInsertResults resultadoInsercao = await table.InsertRowAsync(insertRow);
            Console.WriteLine($"💬 Mensagem ID={novoId} inserida com sucesso!");
        }

        // Inserir uma nova pessoa
        public async Task InserirPessoaAsync(int idPessoa, string nome, string cpf, string email, string senha, string cargo, string status)
        {
            string tableId = "Pessoas";
            var table = _client.GetTable(_datasetId, tableId);

            var insertRow = new BigQueryInsertRow
            {
                { "ID_Pessoa", idPessoa },
                { "Nome_Pessoa", nome },
                { "CPF", cpf },
                { "Email", email },
                { "Senha", senha },
                { "Cargo", cargo },
                { "Status", status },
                { "Logado", false }
            };

            await table.InsertRowAsync(insertRow);
            Console.WriteLine("👤 Pessoa inserida com sucesso!");
        }

        // Inserir um novo ticket
        public async Task InserirTicketAsync(
            int idColaborador,
            int idAnalista,
            string titulo,
            string descricao,
            string categoria)
        {
            string tableId = "Ticket";

            // Gera o próximo ID
            string sqlId = $"SELECT COALESCE(MAX(id_ticket), 0) + 1 AS novo_id FROM `{_projectId}.{_datasetId}.{tableId}`";
            var result = await _client.ExecuteQueryAsync(sqlId, parameters: null);
            long novoId = 1;
            foreach (var row in result)
                novoId = Convert.ToInt64(row["novo_id"]);

            var table = _client.GetTable(_datasetId, tableId);

            // Usa nomes 100% iguais aos do schema do BigQuery (sensível a maiúsculas)
            var insertRow = new BigQueryInsertRow
            {
                { "id_ticket", novoId },
                { "id_colaborador", idColaborador },
                { "id_analista", idAnalista },
                { "datahora_abertura", DateTime.UtcNow },
                { "datahora_fechamento", null },
                { "titulo_ticket", titulo },
                { "descricao_ticket", descricao },
                { "categoria", categoria },
                { "id_chat", null },
                { "status_ticket", "Aberto" }
            };

            await table.InsertRowAsync(insertRow);
            Console.WriteLine($"🎫 Ticket {novoId} inserido com sucesso!");
        }

        // ======== EXEMPLO DE USO NO PROGRAMA PRINCIPAL ========
        // ==============================================
        class Program
        {
            static async Task Main()
            {
                var bq = new BigQueryConnector.BigQueryConnection();
                bq.Connect();

                // ➤ Inserir uma mensagem no Chat
                await bq.InserirMensagemAsync(1001, 1, 2, "Olá, tudo bem?");

                // ➤ Inserir um novo usuário
                await bq.InserirPessoaAsync(3, "Ana Souza", "12345678900", "ana@empresa.com", "senha123", "Analista", "Ativo");

                // ➤ Inserir ticket novo
                await bq.InserirTicketAsync(1, 2, "Erro no sistema", "Não consigo acessar o painel", "Suporte Técnico");

                // ➤ Fazer uma consulta simples
                var mensagens = await bq.GetMensagensAsync();
                Console.WriteLine($"Total de mensagens: {mensagens.Count}");

                var pessoas = await bq.GetPessoasAsync();
                Console.WriteLine($"Total de pessoas: {pessoas.Count}");

                var tickets = await bq.GetTicketsAbertosAsync();
                Console.WriteLine($"Tickets abertos: {tickets.Count}");
            }
        } // <-- Fecha a classe Program
    } // <-- Fecha a classe BigQueryConnection
} // <-- Fecha o namespace BigQueryConnector