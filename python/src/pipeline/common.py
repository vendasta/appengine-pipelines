#!/usr/bin/env python
#
# Copyright 2010 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Common Pipelines for easy reuse."""

import logging

from . import pipeline


class Return(pipeline.Pipeline):
  """Causes calling generator to have the supplied default output value.

  Only works when yielded last!
  """

  def run(self, return_value=None):
    return return_value


class Ignore(pipeline.Pipeline):
  """Mark the supplied parameters as unused outputs of sibling pipelines."""

  def run(self, *args):
    pass


class Dict(pipeline.Pipeline):
  """Returns a dictionary with the supplied keyword arguments."""

  def run(self, **kwargs):
    return dict(**kwargs)


class List(pipeline.Pipeline):
  """Returns a list with the supplied positional arguments."""

  def run(self, *args):
    return list(args)


class AbortIfTrue(pipeline.Pipeline):
  """Aborts the entire pipeline if the supplied argument is True."""

  def run(self, value, message=''):
    if value:
      raise pipeline.Abort(message)


class All(pipeline.Pipeline):
  """Returns True if all of the values are True.

  Returns False if there are no values present.
  """

  def run(self, *args):
    if len(args) == 0:
      return False
    for value in args:
      if not value:
        return False
    return True


class Any(pipeline.Pipeline):
  """Returns True if any of the values are True."""

  def run(self, *args):
    for value in args:
      if value:
        return True
    return False


class Complement(pipeline.Pipeline):
  """Returns the boolean complement of the values."""

  def run(self, *args):
    if len(args) == 1:
      return not args[0]
    else:
      return [not value for value in args]


class Max(pipeline.Pipeline):
  """Returns the max value."""

  def __init__(self, *args):
    if len(args) == 0:
      raise TypeError('max expected at least 1 argument, got 0')
    pipeline.Pipeline.__init__(self, *args)

  def run(self, *args):
    return max(args)


class Min(pipeline.Pipeline):
  """Returns the min value."""

  def __init__(self, *args):
    if len(args) == 0:
      raise TypeError('min expected at least 1 argument, got 0')
    pipeline.Pipeline.__init__(self, *args)

  def run(self, *args):
    return min(args)


class Sum(pipeline.Pipeline):
  """Returns the sum of all values."""

  def __init__(self, *args):
    if len(args) == 0:
      raise TypeError('sum expected at least 1 argument, got 0')
    pipeline.Pipeline.__init__(self, *args)

  def run(self, *args):
    return sum(args)


class Multiply(pipeline.Pipeline):
  """Returns all values multiplied together."""

  def __init__(self, *args):
    if len(args) == 0:
      raise TypeError('multiply expected at least 1 argument, got 0')
    pipeline.Pipeline.__init__(self, *args)

  def run(self, *args):
    total = 1
    for value in args:
      total *= value
    return total


class Negate(pipeline.Pipeline):
  """Returns each value supplied multiplied by -1."""

  def __init__(self, *args):
    if len(args) == 0:
      raise TypeError('negate expected at least 1 argument, got 0')
    pipeline.Pipeline.__init__(self, *args)

  def run(self, *args):
    if len(args) == 1:
      return -1 * args[0]
    else:
      return [-1 * x for x in args]


class Extend(pipeline.Pipeline):
  """Combine together lists and tuples into a single list.

  Args:
    *args: One or more lists or tuples.

  Returns:
    A single list of all supplied lists merged together in order. Length of
    the output list is the sum of the lengths of all input lists.
  """

  def run(self, *args):
    combined = []
    for value in args:
      combined.extend(value)
    return combined


class Append(pipeline.Pipeline):
  """Combine together values into a list.

  Args:
    *args: One or more values.

  Returns:
    A single list of all values appended to the same list. Length of the
    output list matches the length of the input list.
  """

  def run(self, *args):
    combined = []
    for value in args:
      combined.append(value)
    return combined


class Concat(pipeline.Pipeline):
  """Concatenates strings together using a join character.

  Args:
    *args: One or more strings.
    separator: Keyword argument only; the string to use to join the args.

  Returns:
    The joined string.
  """

  def run(self, *args, **kwargs):
    separator = kwargs.get('separator', '')
    return separator.join(args)


class Union(pipeline.Pipeline):
  """Like Extend, but the resulting list has all unique elements."""

  def run(self, *args):
    combined = set()
    for value in args:
      combined.update(value)
    return list(combined)


class Intersection(pipeline.Pipeline):
  """Returns only those items belonging to all of the supplied lists.

  Each argument must be a list. No individual items are permitted.
  """

  def run(self, *args):
    if not args:
      return []
    result = set(args[0])
    for value in args[1:]:
      result.intersection_update(set(value))
    return list(result)


class Uniquify(pipeline.Pipeline):
  """Returns a list of unique items from the list of items supplied."""

  def run(self, *args):
    return list(set(args))


class Format(pipeline.Pipeline):
  """Formats a string with formatting arguments."""

  @classmethod
  def dict(cls, message, **format_dict):
    """Formats a dictionary.

    Args:
      message: The format string.
      **format_dict: Keyword arguments of format parameters to use for
        formatting the string.

    Returns:
      The formatted string.
    """
    return cls('dict', message, format_dict)

  @classmethod
  def tuple(cls, message, *params):
    """Formats a tuple.

    Args:
      message: The format string.
      *params: The formatting positional parameters.

    Returns:
      The formatted string.
    """
    return cls('tuple', message, *params)

  def run(self, format_type, message, *params):
    if format_type == 'dict':
      return message % params[0]
    elif format_type == 'tuple':
      return message % params
    else:
      raise pipeline.Abort('Invalid format type: %s' % format_type)


class Log(pipeline.Pipeline):
  """Logs a message, just like the Python logging module."""

  # TODO: Hack the call stack of the logging message to use the file and line
  # context from when it was first scheduled, not when it actually ran.

  _log_method = logging.log

  @classmethod
  def log(cls, *args, **kwargs):
    return Log(*args, **kwargs)

  @classmethod
  def debug(cls, *args, **kwargs):
    return Log(logging.DEBUG, *args, **kwargs)

  @classmethod
  def info(cls, *args, **kwargs):
    return Log(logging.INFO, *args, **kwargs)

  @classmethod
  def warning(cls, *args, **kwargs):
    return Log(logging.WARNING, *args, **kwargs)

  @classmethod
  def error(cls, *args, **kwargs):
    return Log(logging.ERROR, *args, **kwargs)

  @classmethod
  def critical(cls, *args, **kwargs):
    return Log(logging.CRITICAL, *args, **kwargs)

  def run(self, level, message, *args):
    Log._log_method(level, message, *args)
