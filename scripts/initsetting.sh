# 1. 디스크 목록 확인
sudo fdisk -l

# 2. 마운트할 디스크가 있다면 (예: /dev/sda)
sudo mount /dev/sdb /home/grey1/stock-kafka3/data

# 3. 마운트 확인
df -h /home/grey1/stock-kafka3/data