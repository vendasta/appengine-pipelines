from invoke import task
import invoke

@task()
def serve(ctx):
    """
    Serve the application
    """
    args = [
        'dev_appserver.py',
        '--python_virtualenv_path=./venv',
        '--clear_datastore=yes',
        './demo'
    ]
    invoke.run(" ".join(args))
