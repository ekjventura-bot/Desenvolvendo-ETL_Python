# Projeto de Conta Bancária com ETL

# --- Dados fictícios dos usuários ---
usuarios = [
    {"nome": "Alice", "conta": "001", "saldo": 1500.75},
    {"nome": "Bruno", "conta": "002", "saldo": 250.00},
    {"nome": "Carla", "conta": "003", "saldo": 3200.10}
]

# --- Função de Extração ---
def extrair_mensagens():
    # Mensagens genéricas que o banco deseja enviar
    return [
        "Seu saldo foi atualizado.",
        "Promoção especial para clientes premium!",
        "Aviso de segurança: mantenha seus dados protegidos."
    ]

# --- Função de Transformação ---
def transformar_mensagens(mensagens, usuarios):
    mensagens_transformadas = []
    for usuario in usuarios:
        for msg in mensagens:
            mensagem_personalizada = f"Olá {usuario['nome']} (Conta {usuario['conta']}): {msg} Saldo atual: R${usuario['saldo']:.2f}"
            mensagens_transformadas.append(mensagem_personalizada)
    return mensagens_transformadas

# --- Função de Carregamento ---
def carregar_mensagens(mensagens_transformadas):
    print("\n--- Enviando mensagens para os clientes ---\n")
    for mensagem in mensagens_transformadas:
        print(mensagem)

# --- Fluxo ETL ---
def fluxo_etl():
    mensagens = extrair_mensagens()              # Extração
    mensagens_transformadas = transformar_mensagens(mensagens, usuarios)  # Transformação
    carregar_mensagens(mensagens_transformadas)  # Carregamento

# --- Executar o projeto ---
if __name__ == "__main__":
    fluxo_etl()