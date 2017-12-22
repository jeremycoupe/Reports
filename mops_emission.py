"""Emission and excess emission computation module for ATD-2 mops package.
"""

__author__ = "Amir H Farrahi"
__email__ = "amir.h.farrahi@nasa.gov"

#-------------------------------------------------------------------------------------------------------------------------------------

import os.path
import sys
import pandas as pd

#-------------------------------------------------------------------------------------------------------------------------------------

_emission_df		= None

#-------------------------------------------------------------------------------------------------------------------------------------
# Given a dataFrame row containing the columns:  'aircraftType', 'weightClass', 'MoveTAct', 'RampTAct', 'TaxiTAct', this function
# returns the overall emissions totals (txFuel, txCo, txCo2, txHc, txNox), the ramp emission totals (rmpfuel, rmpCo, rmpC02, rmpHc,
# rmpNox) and the movement area emission totals (mvFuel, mvCo, mvCo2, mvHc, mvNox) back to the caller in a pd.Series structire.
#-------------------------------------------------------------------------------------------------------------------------------------

def row_get_total_emission(row):
    if row.TaxiTAct > 0:
        txFuel, txCo, txCo2, txHc, txNox = getEmissionsForInterval(row.aircraftType, row.weightClass, row.TaxiTAct)
    else:
        txFuel, txCo, txCo2, txHc, txNox = 0, 0, 0, 0, 0

    if row.RampTAct > 0:
        rmpFuel, rmpCo, rmpCo2, rmpHc, rmpNox = getEmissionsForInterval(row.aircraftType, row.weightClass, row.RampTAct)
    else:
        rmpFuel, rmpCo, rmpCo2, rmpHc, rmpNox = 0, 0, 0, 0, 0

    if row.MoveTAct > 0:
        mvFuel, mvCo, mvCo2, mvHc, mvNox = getEmissionsForInterval(row.aircraftType, row.weightClass, row.MoveTAct)
    else:
        mvFuel, mvCo, mvCo2, mvHc, mvNox = 0, 0, 0, 0, 0
    return pd.Series([txFuel, txCo, txCo2, txHc, txNox, rmpFuel, rmpCo, rmpCo2, rmpHc, rmpNox, mvFuel, mvCo, mvCo2, mvHc, mvNox])

#-------------------------------------------------------------------------------------------------------------------------------------
# Given a dataFrame row containing the columns:  'aircraftType', 'weightClass', 'MoveDelay', 'RampDelay', 'TaxiDealy', this function
# returns the overall excess emissions (txFuel, txCo, txCo2, txHc, txNox), the ramp excess emissions (rmpfuel, rmpCo, rmpC02, rmpHc,
# rmpNox) and the movement area excess emissions (mvFuel, mvCo, mvCo2, mvHc, mvNox) back to the caller in a pd.Series structire.
#-------------------------------------------------------------------------------------------------------------------------------------
def row_get_excess_emission(row):
    if row.TaxiDelay > 0:
        txFuel, txCo, txCo2, txHc, txNox = getEmissionsForInterval(row.aircraftType, row.weightClass, row.TaxiDelay)
    else:
        txFuel, txCo, txCo2, txHc, txNox = 0, 0, 0, 0, 0

    if row.RampDelay > 0:
        rmpFuel, rmpCo, rmpCo2, rmpHc, rmpNox = getEmissionsForInterval(row.aircraftType, row.weightClass, row.RampDelay)
    else:
        rmpFuel, rmpCo, rmpCo2, rmpHc, rmpNox = 0, 0, 0, 0, 0

    if row.MoveDelay > 0:
        mvFuel, mvCo, mvCo2, mvHc, mvNox = getEmissionsForInterval(row.aircraftType, row.weightClass, row.MoveDelay)
    else:
        mvFuel, mvCo, mvCo2, mvHc, mvNox = 0, 0, 0, 0, 0
    return pd.Series([txFuel, txCo, txCo2, txHc, txNox, rmpFuel, rmpCo, rmpCo2, rmpHc, rmpNox, mvFuel, mvCo, mvCo2, mvHc, mvNox])

#-------------------------------------------------------------------------------------------------------------------------------------
# Read and initialize the emission table from the file provide and get ready for future lookup:
#-------------------------------------------------------------------------------------------------------------------------------------

def init_emission(file=None):
    global _emission_df
    if os.path.isfile(file):
        _emission_df = pd.read_csv(file)
        (nRows, nCols) = _emission_df.shape
        print('(I): Read emission_table with {} rows and {} columns.'.format(nRows, nCols))
    else:
        print('(E): init_emission(): Error: Cannot find emission file: {}'.format(file))
        sys.exit()

#-------------------------------------------------------------------------------------------------------------------------------------
# return the following fuelFlow and emission values for given length of running the engine (in seconds) on the surface in the 
# default operating mode:
#
#    fuelFlowKg
#    coGr
#    co2Kg
#    hcGr
#    noxGr
#-------------------------------------------------------------------------------------------------------------------------------------

def getEmissionsForInterval(aircraftType, weightClass, seconds):
    global _emission_df

    if _emission_df is None:
        print('(E): get_fuel_flow(): Emission table needs to be initialized first.  Returning zeros.')
        return 0.0, 0.0, 0.0, 0.0, 0.0
    if aircraftType in _emission_df.aircraftType.values:
        row  = _emission_df[_emission_df.aircraftType == aircraftType]
    else:
        rows = _emission_df[_emission_df.aircraftType == 'Other']
        if weightClass in rows.weightClass.values:
            row = rows[rows.weightClass == weightClass]
        else:
            print('(E): getEmissionForInterval(): Cannot find matching row in the table (aircraftType:{}, weightClass:{})'.format(aircraftType, weightClass))
            print('     returning zeros!')
            return 0.0, 0.0, 0.0, 0.0, 0.0
    fuelFlowKg = seconds * row.fuelFlowKgPerSecond.values[0]
    coGr       = fuelFlowKg * row.coGrPerKgFuelFlow.values[0]
    co2Kg      = fuelFlowKg * row.co2KgPerKgFuelFlow.values[0]
    hcGr       = fuelFlowKg * row.hcGrPerKgFuelFlow.values[0]
    noxGr      = fuelFlowKg * row.noxGrPerKgFuelFlow.values[0]
    return fuelFlowKg, coGr, co2Kg, hcGr, noxGr

#-------------------------------------------------------------------------------------------------------------------------------------

def emissionRow(aircraftType, weightClass):
    if _emission_df is None:
        print('(ERRPR): emissionRow(): Emission table needs to be initialized first.  Abort!')
        sys.exit()
    if aircraftType in _emission_df.aircraftType.values:
        row  = _emission_df[_emission_df.aircraftType == aircraftType]
    else:
        rows = _emission_df[_emission_df.aircraftType == 'Other']
        if weightClass in rows.weightClass.values:
            row = rows[rows.weightClass == weightClass]
        else:
            print('(ERROR): emissionRow(): Could not find aircraftType:{} or weightClass:{} in emission table.  Abort!'.format(aircraftType, weightClass))
    return row

#-------------------------------------------------------------------------------------------------------------------------------------

def aircraft_get_fuel_flow_kg(aircraftType, weightClass, gateHoldSeconds):
    row = emissionRow(aircraftType, weightClass)
    return gateHoldSeconds * row.fuelFlowKgPerSecond.values[0]

#-------------------------------------------------------------------------------------------------------------------------------------

def aircraft_get_co_emission_gr(aircraftType, weightClass, gateHoldSeconds):
    row = emissionRow(aircraftType, weightClass)
    fuelFlowKg = gateHoldSeconds * row.fuelFlowKgPerSecond.values[0]
    coGr       = fuelFlowKg * row.coGrPerKgFuelFlow.values[0]
    return coGr

#-------------------------------------------------------------------------------------------------------------------------------------

def aircraft_get_co2_emission_kg(aircraftType, weightClass, gateHoldSeconds):
    row = emissionRow(aircraftType, weightClass)
    fuelFlowKg = gateHoldSeconds * row.fuelFlowKgPerSecond.values[0]
    co2Kg      = fuelFlowKg * row.co2KgPerKgFuelFlow.values[0]
    return co2Kg

#-------------------------------------------------------------------------------------------------------------------------------------

def aircraft_get_hc_emission_gr (aircraftType, weightClass, gateHoldSeconds):
    row = emissionRow(aircraftType, weightClass)
    fuelFlowKg = gateHoldSeconds * row.fuelFlowKgPerSecond.values[0]
    hcGr       = fuelFlowKg * row.hcGrPerKgFuelFlow.values[0]
    return hcGr

#-------------------------------------------------------------------------------------------------------------------------------------

def aircraft_get_nox_emission_gr (aircraftType, weightClass, gateHoldSeconds):
    row = emissionRow(aircraftType, weightClass)
    fuelFlowKg = gateHoldSeconds * row.fuelFlowKgPerSecond.values[0]
    noxGr      = fuelFlowKg * row.noxGrPerKgFuelFlow.values[0]
    return noxGr

#-------------------------------------------------------------------------------------------------------------------------------------
# Need to finish this self_test.  A very simply manual test done for now.
# def testemissionLookup():
#     fuelFlowTable = {('A319', 'D') : 3.08, ('A320', 'D'): 3.08, ('A321', 'D'): 3.08, ('Other', 'A'): 3.08, ('Other', 'B'): 3.08)
#     hcTable       = (('A319', 'D') : 1.53, ('A320', 'D'): 1.40, ('A321', 'D'): 3.50, ('Other', 'A'): 4.04, ('Other', 'B'): 9.53)
#     coTable       = {('', ''), ('', ''), ('', ''), ('', ''), ('', '')}
#     co2Table      = {('', ''), ('', ''), ('', ''), ('', ''), ('', '')}
#     noxTable      = {('', ''), ('', ''), ('', ''), ('', ''), ('', '')}


#-------------------------------------------------------------------------------------------------------------------------------------

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('----------------------------------------------------------------------------------------------------')
        print('Usage: {} <fuel_and_emission_table_csv_file>'.format(sys.argv[0]))
        print('----------------------------------------------------------------------------------------------------')
        sys.exit()
    emission_fname = sys.argv[1]
    init_emission(emission_fname)

    print('|==================================================================================================|')
    print('|                                 E M I S S I O N    T A B L E                                     |')
    print('+--------------------------------------------------------------------------------------------------+')
    print('|                     Fuel-flow is given in Kg per seconds of engine running                       |')
    print('|                 The emissions HC, CO, NOx are given in Gr per Kg of fuel flow                    |')
    print('|                      The emission CO2 is given in Kg per Kg of fuel flow                         |')
    print('|==================================================================================================|')
    print('  {0:10s}  {1:10s}  {2:12s}  {3:10s}  {4:10s}  {5:10s}  {6:10s}  {7:10s}'.format('a/c Model', 'wtClass', 'engineType', 'HC',  'CO', 'NOx', 'CO2', 'Fuel Flow'))
    print('  {0:10s}  {1:10s}  {2:12s}  {3:10s}  {4:10s}  {5:10s}  {6:10s}  {7:10s}'.format('----------', '----------', '------------', '----------',  '----------', '----------', '----------', '----------'))
    for index, row in _emission_df.iterrows():
        aircraftType = row.aircrafTypet
        weightClass   = row.weightClass
        engineType    = row.assumedEngineType
        fuelFlow      = row.fuelFlowKgPerSecond
        co            = row.coGrPerKgFuelFlow
        co2           = row.co2KgPerKgFuelFlow
        hc            = row.hcGrPerKgFuelFlow
        nox           = row.noxGrPerKgFuelFlow 
        print('  {0:<10s}  {1:<10s}  {2:<12s}  {3:<10.3f}  {4:<10.3f}  {5:<10.3f}  {6:<10.3f}  {7:<10.3f}'.
              format(aircraftType, weightClass, engineType, hc, co, nox, co2, fuelFlow))
    print('====================================================================================================')