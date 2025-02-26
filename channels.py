from rules import check_limits, check_pulse, check_water_pulse

class Channel:
    def __init__(self, name:str, min_value:float, max_value:float, regex:str, check_func:callable=None):
        self.name = name
        self.min_value = min_value
        self.max_value = max_value
        self.regex = regex
        self.check_func = check_func

    def __str__(self):
        return f"Channel {self.name}"
    
    def __repr__(self):
        return f"Channel {self.name}, {self.min_value}-{self.max_value}, {self.regex}\n"
    
    def check_channel(self, data, unit_no, bad_indices):
        errors = []
        warnings = []
        if self.check_func:
            errors, warnings = self.check_func(self.regex, data, self.min_value, self.max_value, unit_no, bad_indices)
        return errors, warnings

channels = {
    "A/C Watts": Channel("A/C Watts", 0, 3500, "A/C (Watts)$", check_limits),
    "AHU Watts": Channel("AHU Watts", 0, 400, "AHU (Watts)$", check_pulse),
    "Baseboard Heater 1 Watts": Channel("Baseboard Heater 1 Watts", 0, 2000, "Baseboard.*Heater.*1.*(Watts)$", check_limits),
    "Baseboard Heater 2 Watts": Channel("Baseboard Heater 2 Watts", 0, 2000, "Baseboard.*Heater.*2.*(Watts)$", check_limits),
    "Baseboard Heater 3 Watts": Channel("Baseboard Heater 3 Watts", 0, 2000, "Baseboard.*Heater.*3.*(Watts)$", check_limits),
    "Bedroom Plugs Watts": Channel("Bedroom Plugs Watts", 0, 3000, "Bedroom.?Plugs.*(Watts)$", check_limits),
    "Dishwasher Watts": Channel("Dishwasher Watts", 0, 1200, "Dishwasher.*(Watts)$", check_limits),
    "Dryer (1) Watts": Channel("Dryer (1) Watts", 0, 3500, "Dryer.*1.*(Watts)$", check_limits),
    "Dryer (2) Watts": Channel("Dryer (2) Watts", 0, 3500, "Dryer.*2.*(Watts)$", check_limits),
    "Electrical Baseboard 1 Watts": Channel("Electrical Baseboard 1 Watts", 0, 2000, "Electrical.*Baseboard1.*(Watts)$", check_limits),
    "Electrical Baseboard 2 Watts": Channel("Electrical Baseboard 2 Watts", 0, 2000, "Electrical.*Baseboard2.*(Watts)$", check_limits),
    "Electrical Baseboard 3 Watts": Channel("Electrical Baseboard 3 Watts", 0, 2000, "Electrical.*Baseboard3.*(Watts)$", check_limits),
    "Electrical Baseboard 4 Watts": Channel("Electrical Baseboard 4 Watts", 0, 2000, "Electrical.*Baseboard4.*(Watts)$", check_limits),
    "Fridge Watts": Channel("Fridge Watts", 0, 627.2, "Fridge.*(Watts)$", check_pulse),
    "Ground Level Plugs Watts": Channel("Ground Level Plugs Watts", 0, 3000, "Ground.*Level.*Plugs?.*(Watts)$", check_limits),
    "HRV Watts": Channel("HRV Watts", 0, 500, "HRV.*(Watts)$", check_limits),
    "Hot Water Tank 1 Watts": Channel("Hot Water Tank 1 Watts", 0, 4500, "Hot.*Water.*Tank.*1.*(Watts)$", check_limits),
    "Hot Water Tank 2 Watts": Channel("Hot Water Tank 2 Watts", 0, 4500, "Hot.*Water.*Tank.*2.*(Watts)$", check_limits),
    "Kitchen Counter Plugs Watts": Channel("Kitchen Counter Plugs Watts", 0, 3000, "Kitchen.*Counter.*Plug?.*(Watts)$", check_limits),
    "Living Room Plugs Watts": Channel("Living Room Plugs Watts", 0, 3000, "Living.*Room.*Plugs?.*(Watts)$", check_limits),
    "Main Electricity 1 Watts": Channel("Main Electricity 1 Watts", 0, 10000, "Main\\s*Electricity\\s*1\\s*(Watts)$", check_limits),
    "Main Electricity 2 Watts": Channel("Main Electricity 2 Watts", 0, 10000, "Main\\s*Electricity\\s*2\\s*(Watts)$", check_limits),
    "Main Electricity Gen Watts": Channel("Main Electricity Gen Watts", -10000, 10000, "Main\\s*Electricity\\s*Gen\\s*(Watts)$", check_limits),
    "Main Electricity Gen Watts 1": Channel("Main Electricity Gen Watts 1", -10000, 10000, "Main\\s*Electricity\\s*Gen\\s*Watts.*1$", check_limits),
    "Main Floor Plugs Watts": Channel("Main Floor Plugs Watts", 0, 3000, "Main.*Floor.*Plugs?.*(Watts)$", check_limits),
    "Office Room Plugs Watts": Channel("Office Room Plugs Watts", 0, 3000, "Office.*Room.*Plugs?.*(Watts)$", check_limits),
    "PV Generation 1 Watts": Channel("PV Generation 1 Watts", 0, 1500, "PV.*Generation.*1.*(Watts)$", check_limits),
    "PV Generation 2 Watts": Channel("PV Generation 2 Watts", 0, 1500, "PV.*Generation.*2.*(Watts)$", check_limits),
    "Range (1) Watts": Channel("Range (1) Watts", 0, 11300, "Range.*1.*(Watts)$", check_limits),
    "Range (2) Watts": Channel("Range (2) Watts", 0, 11300, "Range.*2.*(Watts)$", check_limits),
    "Second Floor Plugs Watts": Channel("Second Floor Plugs Watts", 0, 3000, "Second.*Floor.*Plugs?.*(Watts)$", check_limits),
    "Tankless WaterHeater Watts": Channel("Tankless WaterHeater Watts", 0, 250, "Tankless.*WaterHeater.*(Watts)$", check_limits),
    "Washing Machine Watts": Channel("Washing Machine Watts", 0, 1000, "Washing.*Machine.*(Watts)$", check_limits),
    "Return Air Avg C": Channel("Return Air Avg C", 0.0001, 35, "Return.*Air.*Avg.*C$", check_limits),
    "Cold Water Avg C": Channel("Cold Water Avg C", 0.0001, 27, "Cold.*Water.*Avg.*C$", check_limits),
    "Heat Recovery Water Avg C": Channel("Heat Recovery Water Avg C", 0.0001, 45, "Heat.*C$", check_limits),
    "Hot Water Avg C": Channel("Hot Water Avg C", 0.0001, 65, "Hot.*Water.*Avg.*C$", check_limits),
    "Volts": Channel("Volts", 60, 180, "Volts", check_limits),
    "Cold Water Cubic Meter": Channel("Cold Water Cubic Meter", 0, 25, "Cold.*Water.*Cubic.*(Meter)$", check_water_pulse),
    "Hot Water Cubic Meter": Channel("Hot Water Cubic Meter", 0, 25, "Hot.*Water.*Cubic.*(Meter)$", check_water_pulse),
    "Natural Gas": Channel("Natural Gas", 0, 25, "Natural.*Gas", check_pulse)
}