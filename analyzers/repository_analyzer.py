import os
import tempfile
import shutil
from git import Repo
from git.exc import GitCommandError, InvalidGitRepositoryError

class RepositoryAnalyzer:
    """Handles repository operations for both Git URLs and local directories."""
    
    def __init__(self):
        self.temp_dirs = []
    
    def clone_repository(self, git_url):
        """Clone a Git repository to a temporary directory."""
        try:
            temp_dir = tempfile.mkdtemp()
            self.temp_dirs.append(temp_dir)
            
            # Clone the repository
            Repo.clone_from(git_url, temp_dir)
            return temp_dir
            
        except (GitCommandError, InvalidGitRepositoryError) as e:
            raise Exception(f"Failed to clone repository: {str(e)}")
        except Exception as e:
            raise Exception(f"Unexpected error during cloning: {str(e)}")
    
    def validate_local_directory(self, directory_path):
        """Validate that a local directory exists and is accessible."""
        if not os.path.exists(directory_path):
            raise Exception(f"Directory does not exist: {directory_path}")
        
        if not os.path.isdir(directory_path):
            raise Exception(f"Path is not a directory: {directory_path}")
        
        if not os.access(directory_path, os.R_OK):
            raise Exception(f"Directory is not readable: {directory_path}")
        
        return True
    
    def get_python_files(self, directory):
        """Get all Python files in the directory recursively."""
        python_files = []
        
        for root, dirs, files in os.walk(directory):
            # Skip common non-source directories
            dirs[:] = [d for d in dirs if d not in ['.git', '__pycache__', '.pytest_cache', 'node_modules', '.venv', 'venv']]
            
            for file in files:
                if file.endswith('.py'):
                    python_files.append(os.path.join(root, file))
        
        return python_files
    
    def cleanup_temp_directories(self):
        """Clean up all temporary directories created during analysis."""
        for temp_dir in self.temp_dirs:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
        self.temp_dirs = []
    
    def get_repository_info(self, directory):
        """Get repository information if it's a Git repository."""
        try:
            repo = Repo(directory)
            return {
                'is_git_repo': True,
                'branch': repo.active_branch.name,
                'commit_hash': repo.head.commit.hexsha,
                'commit_message': repo.head.commit.message.strip(),
                'remote_url': next(iter(repo.remotes)).url if repo.remotes else None
            }
        except (InvalidGitRepositoryError, Exception):
            return {'is_git_repo': False}