##############################################################################
Data Quality Guide
##############################################################################

This guide covers data quality validation using Great Expectations in the Engineering Data Platform.

================================================================================
Overview
================================================================================

The platform implements a **Data Quality** layer that validates all engineering units against realistic bounds before data reaches the database. This ensures data integrity and prevents invalid measurements from corrupting analytics.

================================================================================
Engineering Unit Bounds
================================================================================

Wind Data
=========

.. list-table::
   :header-rows: 1

   * - Parameter
     - Min
     - Max
     - Unit
     - Description
   * - ``wind_speed``
     - 0
     - 50
     - m/s
     - Cannot be negative; extreme hurricane is ~50 m/s
   * - ``wind_direction``
     - 0
     - 360
     - degrees
     - Compass direction
   * - ``temperature``
     - -50
     - 60
     - °C
     - Extreme cold to hot desert
   * - ``pressure``
     - 80,000
     - 110,000
     - Pa
     - Atmospheric pressure range
   * - ``air_density``
     - 0.9
     - 1.5
     - kg/m³
     - Standard atmosphere density
   * - ``turbulence_intensity``
     - 0
     - 1
     - -
     - Normalized turbulence

Atmospheric Data
================

.. list-table::
   :header-rows: 1

   * - Parameter
     - Min
     - Max
     - Unit
     - Description
   * - ``co2_concentration``
     - 300
     - 600
     - ppm
     - Pre-industrial to elevated
   * - ``ch4_concentration``
     - 1,500
     - 2,500
     - ppb
     - Methane concentration
   * - ``ozone_concentration``
     - 100
     - 600
     - Dobson units
     - Ozone layer
   * - ``aerosol_optical_depth``
     - 0
     - 5
     - -
     - Atmospheric clarity
   * - ``surface_temperature``
     - 200
     - 350
     - K
     - Kelvin (freezing to hot)
   * - ``sea_level_pressure``
     - 90,000
     - 110,000
     - Pa
     - Sea level pressure
   * - ``wind_u``
     - -50
     - 50
     - m/s
     - Zonal wind component
   * - ``wind_v``
     - -50
     - 50
     - m/s
     - Meridional wind component

Location Data
=============

.. list-table::
   :header-rows: 1

   * - Parameter
     - Min
     - Max
     - Unit
   * - ``location_lat``
     - -90
     - 90
     - degrees
   * - ``location_lon``
     - -180
     - 180
     - degrees

================================================================================
Great Expectations Configuration
================================================================================

Expectations File
=================

Location: ``data-ingestion/quality-control/great_expectations/expectations.py``

.. code-block:: python

   ENGINEERING_BOUNDS = {
       "wind_speed": {
           "min": 0.0,
           "max": 50.0,
           "description": "Wind speed must be between 0 and 50 m/s"
       },
       # ... more bounds
   }

Running Validation
==================

Command Line
------------

.. code-block:: bash

   # Run checkpoint
   docker exec -it great-expectations gx checkpoint run energy_data_checkpoint

   # Run with verbose output
   docker exec -it great-expectations gx checkpoint run energy_data_checkpoint --verbose

Python API
----------

.. code-block:: python

   from quality_control.validators.energy_data_validator import DataQualityValidator

   validator = DataQualityValidator()

   # Validate wind data
   wind_result = validator.validate_wind_data(wind_data)

   # Validate atmospheric data
   atm_result = validator.validate_atmospheric_data(atmospheric_data)

   # Run full validation
   full_result = validator.run_full_validation(
       wind_data=wind_data,
       atmospheric_data=atmospheric_data
   )

================================================================================
Validation Results
================================================================================

Validation results are stored in the ``gx/uncommitted/`` directory and can be viewed through the Great Expectations UI:

.. code-block:: bash

   docker exec -it great-expectations gx suite show <suite_name>
