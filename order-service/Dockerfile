# 1. Use a lightweight Node.js image
FROM node:18-alpine

# 2. Create the directory where the app lives inside the container
WORKDIR /app

# 3. Copy the package files first (to cache dependencies)
COPY package*.json ./

# 4. Install the Node modules
RUN npm install

# 5. Copy the rest of your service code (index.js, etc.)
COPY . .

# 6. Open port 5000 (standard for our Order Service)
EXPOSE 5000

# 7. Start the application
CMD ["node", "index.js"]