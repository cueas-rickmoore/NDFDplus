
class Constant(object):
    def __init__(self, constants, Symbol, value, units, description):
        constants.symbols[Symbol] = self
        self._description = description
        self._units = units 
        self._value = value
    @property
    def description(self): return self._description
    @property
    def u(self): return self._units
    @property
    def v(self): return self._value

class Constants(object):
  def __init__(self):
    self.symbols = { }
    # Universal constants
    self.Cd_p = Constant(self, 'Cdp', 1005.7, 'kg K',
                         'Heat capacity of dry air at constant pressure')
    self.Cd_v = Constant(self, 'Cdv', 719.0, 'kg K',
                         'Heat capacity of dry air at constant volume')
    self.Cd_l = Constant(self, 'Cdv', 4190.0, 'kg K',
                         'Heat capacity of liquid water')
    self.Cv_p = Constant(self, 'Cdp', 1870.0, 'kg K',
                         'Heat capacity of water vapor at constant pressure')
    self.Cv_v = Constant(self, 'Cdv', 1410.0, 'kg K',
                         'Heat capacity of water vapor at constant volume')
    self.G = Constant(self, 'G', 6.67e-11, 'N m^2 kq^-2',
                      'Universal gravitational constant')
    self.Lv = Constant(self, 'Lv', 2.26E6, None,
                      'Latent heat of vaporization for water at 100 deg C')
    self.Lv0 = Constant(self, 'Lv', 2.501E6, None,
                      'Latent heat of vaporization for water at 0 deg C')
    self.R = Constant(self, 'R*', 8.3143, 'J K^-1 mol^-1',
                      'Universal gas constatnt (SI units)')
    self.Rc = Constant(self, 'Rc*', 0.0821, 'L atm K^-1 mol^-1',
                       'Gas constant in chemical units')
    self.Rd = Constant(self, 'Rd', 287.04, 'J kg^-1 K^-1',
                       'Gas constant of dry air')
    self.Rv = Constant(self, 'Rd', 461.50, 'J kg^-1 K^-1',
                       'Gas constant of water vapor')
    self.c = Constant(self, 'c', 2.998e8, 'm s^-1', 'Speed of light')
    self.g = Constant(self, 'g', 9.81, 'm s^-2',
                      'acceleration due to gravity')
    self.eta = Constant(self, 'u\x03B7', 6.626e-34, 'J s', "Plank's constant")
    self.epsilon = Constant(self, 'u\x03B5', 0.6220, None, 'ratio of Rd/Rv')
    self.sigma = Constant(self, 'u\x03C3', 0, '', '')

    #self.g0 = Constant(9.81, 'N.kg-1', 'Acceleration due to gravity at seas level')

  def __getitem__(self, key):
    return self.symbols[key]

CONSTANTS = Constants()
