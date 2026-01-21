# Use Node 22 (LTS) to satisfy Vite's requirements
FROM node:22-alpine

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy the rest of the source
COPY . .

# Vite default port
EXPOSE 5173

# Start Vite with --host to allow external access (Docker)
CMD ["npm", "run", "dev", "--", "--host"]