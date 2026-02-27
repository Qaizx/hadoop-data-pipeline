#!/bin/bash
# สร้าง self-signed certificate สำหรับ HTTPS
# รันครั้งเดียว แล้ว commit ไฟล์ .crt ไว้ (ไม่ต้อง commit .key)

mkdir -p certs

openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
    -keyout certs/server.key \
    -out certs/server.crt \
    -subj "/C=TH/ST=ChiangMai/L=ChiangMai/O=ITSC-CMU/CN=localhost" \
    -addext "subjectAltName=IP:127.0.0.1,IP:0.0.0.0,DNS:localhost"

echo "✅ Certificate created in ./certs/"
echo "   server.crt — public certificate (เพิ่มใน .gitignore ได้)"
echo "   server.key — private key (ต้อง .gitignore ห้าม commit!)"
