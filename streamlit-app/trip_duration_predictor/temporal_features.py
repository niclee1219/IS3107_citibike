import holidays

def build_time_features(selected_time):

    us_holidays = holidays.US()

    hour = selected_time.hour
    month = selected_time.month

    is_weekend = selected_time.weekday() >= 5

    is_rush_hour = (
        (7 <= hour <= 9) or
        (17 <= hour <= 19)
    )

    is_holiday = selected_time.date() in us_holidays

    return {
        "hour": hour,
        "month": month,
        "is_weekend": is_weekend,
        "is_rush_hour": is_rush_hour,
        "is_holiday": is_holiday,
    }