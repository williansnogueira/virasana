from virasana.login import login_manager
from virasana.views import app

login_manager.session_protection = 'strong'

if __name__ == '__main__':
    app.run()
