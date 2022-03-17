



def potential(args, input):
    """
        Potential Function for the Kronig-Penney Model
        In this case, it is a periodic square wave
    """
    if input > 0 and input < 1:
        return 0
    elif input > 1 and input < 2:
        return args.H 


