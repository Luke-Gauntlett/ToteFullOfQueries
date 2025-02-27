def format_filepath(date_time, table):

    months = {
        "01": "January",
        "02": "February",
        "03": "March",
        "04": "April",
        "05": "May",
        "06": "June",
        "07": "July",
        "08": "August",
        "09": "September",
        "10": "October",
        "11": "November",
        "12": "December",
    }

    split = date_time.split("-")
    year = split[0]
    month = split[1]
    split2 = split[2].split(" ")
    day = split2[0]
    time = split2[1]

    monthstr = f"{month}-{months[month]}"

    filepath = f"data/by time/{year}/{monthstr}/{day}/{time}/{table}"

    return filepath


def get_data():
    pass
