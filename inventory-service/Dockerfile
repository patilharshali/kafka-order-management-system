# 1. Use a small Node.js runtime
FROM node:18-alpine

# 2. Set the working directory inside the container
WORKDIR /app

# 3. Copy package files first to take advantage of Docker layer caching
COPY package*.json ./

# 4. Install dependencies
RUN npm install

# 5. Copy the source code (index.js)
COPY . .

# 6. Run the worker
CMD ["npm", "start"]