#!/bin/sh
# 상위 디렉토리로 이동
cd ../

# output 디렉토리 생성
mkdir -p output  # 이미 존재하는 경우에도 에러 없이 넘어감

# FastAPI 레포지토리 내의 파일을 output으로 복사
cp -R ./FastAPI/* ./output

# 복사한 output을 FastAPI 레포지토리로 다시 복사 (필요한 경우)
cp -R ./output ./FastAPI/
