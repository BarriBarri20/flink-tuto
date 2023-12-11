# Big Data Project

This project uses Docker and Grafana to visualize data of 5 patients and then calculate the average glucose of each user every hour.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

- Docker
- Docker Compose

### Installation

1. Clone the repository
```bash
git clone https://github.com/BarriBarri20/flink-tuto
```

2. Navigate to the project directory
```bash
cd table-workthrough
```

3. Build the Docker images
```bash
docker-compose build
```

4. Start the Docker containers
```bash
docker-compose up
```

### Usage

After starting the Docker containers, you can access the Grafana dashboard to visualize your data.

Open your web browser and navigate to:

```bash
http://localhost:3000
```

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details
