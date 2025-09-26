import requests
import config # Importa as configurações do nosso arquivo central

def enviar_mensagem_telegram(mensagem):
    """Envia uma mensagem de notificação para o Telegram."""
    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {'chat_id': config.TELEGRAM_CHAT_ID, 'text': mensagem, 'parse_mode': 'Markdown'}
    try:
        requests.get(url, params=payload, timeout=10)
        print("Mensagem de notificação enviada para o Telegram com sucesso.")
    except Exception as e:
        print(f"Falha ao enviar notificação para o Telegram: {e}")