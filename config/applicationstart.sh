# # 호스트네임(IP 주소) 확인
# current_ip=$(hostname -I | awk '{print $1}')

# # 목표 IP 주소
# target_ip="172.31.30.11"

# # IP 주소가 목표 IP 주소와 일치하는지 확인
# if [ "$current_ip" = "$target_ip" ]; then
#     # 일치하면 Python 스크립트 실행
#     python3 /home/ubuntu/kafka-producer/bicycle-producer/producer.py
# else
#     # 일치하지 않으면 패스
#     echo "IP address does not match. Skipping script execution."
# fi
