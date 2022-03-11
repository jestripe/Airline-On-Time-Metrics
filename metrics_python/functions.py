import pickle

# Contains some functions to help access the database where the data is stored.

auth_path = rf'\home\jestripe\auth.pkl'

def set_psql_auth(uid, pwd):
    try:
        with open(auth_path, 'rb') as f:
            auth = pickle.load(f)
    except (FileNotFoundError, EOFError):
        auth = dict()

    auth = (uid, pwd)
    with open(auth_path, 'wb') as f:
        pickle.dump(auth, f)

def return_psql_auth():
    with open(auth_path, 'rb') as f:
        auth = pickle.load(f)
    return auth