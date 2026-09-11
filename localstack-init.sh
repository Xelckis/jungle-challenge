#!/bin/bash
echo "Inicializando filas SQS FIFO..."
awslocal sqs create-queue --queue-name wager-transactions-dlq.fifo --attributes FifoQueue=true || true
awslocal sqs create-queue --queue-name wager-transactions.fifo --attributes '{"FifoQueue":"true", "RedrivePolicy":"{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:000000000000:wager-transactions-dlq.fifo\",\"maxReceiveCount\":\"3\"}"}' || true
echo "Filas criadas com sucesso."