output "athena_workgroup" {
  description = "Workgroup to use in the Superset connection URI (work_group=...)"
  value       = aws_athena_workgroup.movielens.name
}

output "superset_user" {
  description = "IAM user for Superset. Create its access key manually."
  value       = aws_iam_user.superset.name
}
