import logging, random, sys, time


def exec_action_fct(fct_name, action, db):
    try:
        logging.info("[%s] executing the reconfiguration for the state '%s'" % (
            action.node_name, action.state))
        state_fct = getattr(sys.modules[__name__], fct_name)
        return state_fct(action, db)
    except:
        logging.exception("[%s] action state function error" % action.node_name)
    return False


# Deploy environments
def random_return():
    time.sleep(2)
    nb = random.randrange(0, 12)
    nb_max = 9
    logging.info("random return %s (nb: %d)" % (nb < nb_max, nb))
    print("random return %s (nb: %d)" % (nb < nb_max, nb))
    return nb < nb_max


def deploy1_exec(action, db):
    return random_return()


def deploy2_exec(action, db):
    return random_return()


def deploy2_post(action, db):
    return random_return()


def deploy3_exec(action, db):
    return random_return()


def destroy1_exec(action, db):
    return random_return()


def destroy2_exec(action, db):
    return random_return()


def destroy2_post(action, db):
    return random_return()


def custom1_exec(action, db):
    return random_return()


def custom2_exec(action, db):
    return random_return()


def custom2_post(action, db):
    return random_return()


def reboot1_exec(action, db):
    return random_return()


def reboot2_exec(action, db):
    return random_return()


def reboot2_post(action, db):
    return random_return()


