output "endpoint" {
  description = "The endpoint of the Redshift Serverless cluster"
  value       = module.redshift_serverless.endpoint  
}