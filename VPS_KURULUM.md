# VPS sıfırdan kurulum — v8

Bu paket runtime verisi/SQLite DB içermez. VPS'e temiz proje olarak yüklenir.

## 1) Dosyayı aç

Önerilen konum:

`C:\Sites\kick-live-project`

## 2) Python ortamı

PowerShell:

```powershell
cd C:\Sites\kick-live-project
py -3 -m venv venv
.\venv\Scripts\python.exe -m pip install --upgrade pip
.\venv\Scripts\python.exe -m pip install -r requirements.txt
```

## 3) .env

```powershell
Copy-Item .env.example .env
notepad .env
```

En az:

```text
KICK_CHANNEL_SLUG=rraenee
```

Kullanıyorsan mevcut Kick Client ID/Secret değerlerini de gir.

## 4) Önce servis olmadan test

```powershell
.\venv\Scripts\python.exe -u -m uvicorn server:app --host 127.0.0.1 --port 8000
```

Ayrı PowerShell:

```powershell
Invoke-WebRequest http://127.0.0.1:8000/health | Select-Object -ExpandProperty Content
```

Test bittikten sonra Ctrl+C.

## 5) NSSM

Uygulama:

```text
C:\Sites\kick-live-project\venv\Scripts\python.exe
```

AppDirectory:

```text
C:\Sites\kick-live-project
```

Arguments:

```text
-u -m uvicorn server:app --host 127.0.0.1 --port 8000
```

Bu v8'de `-u` özellikle bırakıldı; servis logları anında yazılır.

Stdout:

```text
C:\Sites\kick-live-project\data\kicklive_stdout.log
```

Stderr:

```text
C:\Sites\kick-live-project\data\kicklive_stderr.log
```

Start: `AUTO`

## 6) Servis sonrası kontrol

```powershell
Get-Service KickLive
Invoke-WebRequest http://127.0.0.1:8000/health | Select-Object -ExpandProperty Content
Invoke-WebRequest http://127.0.0.1:8000/api/status | Select-Object -ExpandProperty Content
```

Canlı DB son mesajını:

```powershell
.\venv\Scripts\python.exe -c "from db import connect; c=connect(); r=c.execute('SELECT COUNT(*) c,MAX(timestamp) last FROM chat_messages').fetchone(); print(dict(r)); c.close()"
```

## v8'de düzeltilen kritik noktalar

- Dashboard WebSocket'i yavaşlasa bile Kick/Pusher socket'i artık beklemiyor.
- WebSocket gönderimleri 2 saniyelik timeout ile korunuyor.
- DB batch'inde tek bir bozuk event bütün kuyruğu kilitlemiyor; batch başarısız olursa eventler tek tek deneniyor.
- Her queue item için `task_done()` garanti ediliyor.
- Recorder bileşenleri supervisor altında; socket/writer/monitor durursa tekrar başlatılıyor.
- `.env` ve DB yolu çalışma klasöründen bağımsız olarak proje klasörüne bağlandı.
- Uvicorn servisinden önce Python'un unbuffered (`-u`) çalışması öneriliyor.
- `/health` ve `/api/status` artık recorder, queue, DB ve son chat durumunu gösteriyor.
- Dashboard WebSocket yeniden bağlandığında son 50 chat mesajını SQLite'dan hydrate ediyor.
