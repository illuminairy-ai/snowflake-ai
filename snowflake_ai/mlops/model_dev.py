# Copyright (c) 2023, Tony Liu
#
# Licensed under the Apache License, Version 2.0 (the "License");
# Use, reproduction and distribution of this software in source and 
# binary forms, with or without modification, are permitted provided that
# the License terms and conditions are met; you may not use this file
# except in compliance with the License. See the LICENSE file for details.

__author__ = "Tony Liu"
__email__ = "tony.liu@yahoo.com"
__license__ = "Apache License 2.0"
__version__ = "0.5.0"

import logging
import numpy as np



class Metrics:

    _logger = logging.getLogger(__name__)

    @staticmethod
    def mean_absolute_percentage_error(y_true, y_pred): 
        y_true, y_pred = np.array(y_true), np.array(y_pred)
        return np.mean(np.abs((y_true - y_pred) / y_true)) * 100