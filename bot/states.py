from aiogram.fsm.state import State, StatesGroup

class DeployStates(StatesGroup):
    waiting_for_dump = State()
    waiting_for_ip = State()
    waiting_for_port = State()
    waiting_for_dbname = State()
    waiting_for_overwrite_confirmation = State()
    waiting_for_username = State()
    waiting_for_password = State()