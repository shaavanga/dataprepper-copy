import boto3
import git
import os
import shutil
import tempfile

def lambda_handler(event, context):
    # AWS CodeCommit client
    codecommit_client = boto3.client('codecommit')
    
    # GitHub repository details
    github_repo_url = "https://github.com/shaavanga/dataprepper-copy.git"
    github_repo_name = "dataprepper-copy"
    github_token = "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"  # Replace with your GitHub access token
    
    # CodeCommit repository name
    codecommit_repo_name = "ShaDataPrepper"  # Replace with your CodeCommit repository name
    
    # Temporary directory to clone GitHub repository
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Clone GitHub repository
        git.Repo.clone_from(github_repo_url, temp_dir)
        
        # Iterate over branches
        repo = git.Repo(temp_dir)
        for branch in repo.branches:
            branch_name = branch.name
            
            # Fetch from GitHub
            remote = repo.remote(name='origin')
            remote.fetch()
            
            # Pull changes from GitHub branch
            remote.pull(branch_name)
            
            # Push code to CodeCommit branch
            codecommit_client.create_branch(
                repositoryName=codecommit_repo_name,
                branchName=branch_name,
                commitId=repo.heads[branch_name].commit.hexsha
            )
            codecommit_client.create_commit(
                repositoryName=codecommit_repo_name,
                branchName=branch_name,
                parentCommitId=repo.heads[branch_name].commit.hexsha,
                authorName=repo.head.commit.author.name,
                email=repo.head.commit.author.email,
                commitMessage=repo.head.commit.message,
                putFiles=[
                    {
                        'filePath': file_path,
                        'fileMode': 'NORMAL',
                        'fileContent': open(os.path.join(temp_dir, file_path), 'rb').read()
                    }
                    for file_path in repo.head.commit.stats.files.keys()
                ]
            )
            
    finally:
        # Clean up temporary directory
        shutil.rmtree(temp_dir)

# Schedule Lambda function to run every 30 minutes using CloudWatch Events
def schedule_lambda(event, context):
    lambda_client = boto3.client('lambda')
    response = lambda_client.create_event_source_mapping(
        EventSourceArn='arn:aws:events:us-west-2:689100289081:rule/gitcc',
        FunctionName=context.function_name,
        Enabled=True
    )
