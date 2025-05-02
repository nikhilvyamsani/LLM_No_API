FROM python:3.10-slim-bullseye

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    git \
    cmake \
    ninja-build \
    build-essential \
    default-libmysqlclient-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Optional: add ccache to speed up future builds
# RUN apt-get install -y ccache

# Setup build flags for llama-cpp-python
ENV CMAKE_ARGS="-DLLAMA_CMAKE_BUILD_TESTS=OFF"
ENV LLAMA_CPP_CMAKE_ARGS="-DLLAMA_AVX=OFF -DLLAMA_FMA=ON -DLLAMA_AVX2=OFF -DLLAMA_F16C=OFF -DLLAMA_NEON=ON"
ENV FORCE_CMAKE=1

# Set working directory
WORKDIR /app

# Upgrade pip & install build tools first
RUN pip install --upgrade pip setuptools wheel scikit-build cmake ninja

# Copy app
COPY ./app /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Expose Streamlit port
EXPOSE 8501

# Run the app
CMD ["streamlit", "run", "TestUI.py", "--server.port=8501", "--server.address=0.0.0.0"]
