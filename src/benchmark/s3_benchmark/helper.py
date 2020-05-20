from os import urandom


def generate_data():
    """
    generate output files
    """
    with open('KiB1', 'wb') as fout:
        fout.write(urandom(1024))
    with open('KiB10', 'wb') as fout:
        fout.write(urandom(10 * 1024))
    with open('KiB100', 'wb') as fout:
        fout.write(urandom(100 * 1024))
    with open('KiB256', 'wb') as fout:
        fout.write(urandom(256 * 1024))
    with open('MiB', 'wb') as fout:
        fout.write(urandom(1024 ** 2))
    with open('MiB10', 'wb') as fout:
        fout.write(urandom(10 * 1024 ** 2))
    with open('MiB100', 'wb') as fout:
        fout.write(urandom(100 * 1024 ** 2))
    with open('GiB', 'wb') as fout:
        fout.write(urandom(1024 ** 3))
    with open('GiB10', 'wb') as fout:
        fout.write(urandom(10 * 1024 ** 3))    


if __name__ == "__main__":
    """
    This is a quick helper that generate output files
    """
    generate_data()
