import os
import re
import imaplib
import email
from email.header import decode_header
import pandas as pd
import json
from datetime import datetime
import logging
from tqdm import tqdm
from threading import Thread
import uuid
import time
import schedule
import signal
import sys

class EmailBackup:
    def __init__(self):
        self.setup_logging()
        self.emails = [
            {
                "email": "abc@abc.com",
                "password": "12345",  
                "imap_server": "abc.com",
                "imap_port": 993
            },
        ]

         # If no emails configured, log warning and exit
        if not self.emails or not any(self.emails):
            logging.warning("No email configurations found. Please add email accounts to backup.")
            sys.exit(1)

    def setup_logging(self):
        logging.basicConfig(
            filename=f'email_backup_{datetime.now().strftime("%Y%m%d")}.log',
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def decode_email_header(self, value):
        try:
            decoded_parts = decode_header(value)
            return ''.join(
                str(part[0], part[1] or 'utf-8') if isinstance(part[0], bytes) else part[0]
                for part in decoded_parts
            )
        except Exception as e:
            logging.error(f"E-posta başlığı decode edilemedi: {str(e)}")
            return value

    def save_attachments(self, msg, backup_dir, message_id):
        attachment_dir = os.path.join(backup_dir, "attachments", message_id)
        os.makedirs(attachment_dir, exist_ok=True)
        attachment_paths = []

        for part in msg.walk():
            try:
                content_disposition = str(part.get("Content-Disposition", ""))
                if "attachment" in content_disposition or part.get_filename():
                    filename = part.get_filename()
                    if filename:
                        # Önce email header'ı decode et
                        filename = self.decode_email_header(filename)
                        
                        # Geçersiz karakterleri temizle
                        invalid_chars = r'[<>:"/\\|?*\r\n]'
                        filename = re.sub(invalid_chars, '_', filename)
                        
                        # Başta ve sondaki boşlukları ve noktaları temizle
                        filename = filename.strip('. ').replace("\r", "").replace("\n", "")
                        
                        # Dosya adının boş olmamasını sağla
                        if not filename:
                            filename = f'attachment_{message_id}'
                        
                        # Dosya adının sayı ile başlamasını engelle
                        if filename[0].isdigit():
                            filename = 'f_' + filename
                        
                        # Maksimum dosya adı uzunluğunu kontrol et
                        max_length = 255
                        if len(filename) > max_length:
                            name, ext = os.path.splitext(filename)
                            filename = name[:max_length-len(ext)] + ext
                        
                        payload = part.get_payload(decode=True)
                        if not payload:
                            logging.warning(f"Eklenti boş veya çözülemedi: {filename}")
                            continue

                        filepath = os.path.join(attachment_dir, filename)
                        
                        # Aynı isimde dosya varsa numaralandır
                        counter = 1
                        while os.path.exists(filepath):
                            name, ext = os.path.splitext(filename)
                            new_filename = f"{name}_{counter}{ext}"
                            filepath = os.path.join(attachment_dir, new_filename)
                            counter += 1

                        with open(filepath, "wb") as f:
                            f.write(payload)
                        attachment_paths.append(filepath)
                        
                        logging.info(f"Kaydedilen dosya adı: {filename}")
                        
            except Exception as e:
                logging.error(f"Eklenti kaydedilemedi: {str(e)}")

        return attachment_paths

    def backup_email(self, email_config):
        """Tek bir e-posta hesabını yedekler."""
        try:
            mail = imaplib.IMAP4_SSL(email_config["imap_server"], email_config["imap_port"])
            mail.login(email_config["email"], email_config["password"])

            email_username = email_config["email"].split('@')[0]
            backup_dir = f'backup_{email_username}'
            if not os.path.exists(backup_dir):
                os.makedirs(backup_dir)

            mail_data = []

            logging.info(f"Giriş yapıldı: {email_config['email']}")
            _, folders = mail.list()

            for folder in folders:
                folder_name = folder.decode().split(' "/" ')[-1].strip()
                logging.info(f"İşlenen klasör: {folder_name}")
                mail.select(folder_name, readonly=True)

                _, messages = mail.search(None, 'ALL')
                if not messages[0]:
                    continue

                for num in tqdm(messages[0].split(), desc=f"Processing {folder_name}"):
                    try:
                        _, msg_data = mail.fetch(num, '(RFC822)')
                        msg = email.message_from_bytes(msg_data[0][1])
                        unique_id = str(uuid.uuid4()) 
                        timestamp = int(time.time() * 1000)
                        message_id = f"{unique_id}_{timestamp}"  
    
                        body = ""
                        if msg.is_multipart():
                            for part in msg.walk():
                                if part.get_content_type() == "text/plain":
                                    body = part.get_payload(decode=True).decode(errors="replace")
                                    break
                        else:
                            body = msg.get_payload(decode=True).decode(errors="replace")

                        attachments = self.save_attachments(msg, backup_dir, message_id)
                        mail_info = {
                            'folder': folder_name,
                            'subject': self.decode_email_header(msg.get('subject', '')),
                            'from': self.decode_email_header(msg.get('from', '')),
                            'to': self.decode_email_header(msg.get('to', '')),
                            'received_headers': [header for header in msg.get_all('Received', [])],
                            'date': msg.get('date', ''),
                            'body': body,
                            'cc': self.decode_email_header(msg.get('CC', '')),
                            'error_message': msg.get('X-Failed-Recipients', None) or msg.get('Diagnostic-Code', None),
                            'smtp_error_code': msg.get('X-SMTP-Error', None),
                            'attachments': attachments
                        }
                        mail_data.append(mail_info)
                    except Exception as e:
                        logging.error(f"Mail işlenirken hata: {str(e)}")

            df = pd.DataFrame(mail_data)
            df.to_excel(os.path.join(backup_dir, "emails.xlsx"), index=False)
            with open(os.path.join(backup_dir, "emails.json"), "w", encoding="utf-8") as f:
                json.dump(mail_data, f, ensure_ascii=False, indent=2)

            mail.logout()
            logging.info(f"Backup tamamlandı: {email_config['email']}")
        except Exception as e:
            logging.error(f"Backup hatası: {str(e)}")

    def backup_all_emails(self):
        """Tüm e-posta hesaplarını yedekler."""
        threads = []
        for email_config in self.emails:
            thread = Thread(target=self.backup_email, args=(email_config,))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()
        logging.info("Tüm e-posta yedeklemeleri tamamlandı.")


if __name__ == "__main__":
    backup = EmailBackup()    
    # Add signal handler for graceful shutdown
    def signal_handler(sig, frame):
        logging.info("Program sonlandırılıyor...")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    # Run backup immediately first time
    backup.backup_all_emails()
    
    # Then schedule periodic backups
    schedule.every(720).minutes.do(backup.backup_all_emails)
    
    logging.info("Backup servisi çalışıyor...")
    while True:
        schedule.run_pending()
        time.sleep(1)
