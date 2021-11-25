def _dict_subset(d1, d2):
  assert isinstance(d1, dict)
  assert isinstance(d2, dict)

  for k in d1:
    if k not in d2:
      return False
    if not isinstance(d1[k], dict):
      if d1[k] != d2[k]:
        return False
    else:
      if not isinstance(d2[k], dict):
        return False
      if not _dict_subset(d1[k], d2[k]):
        return False
  return True

#assert _dict_subset({'a':5}, {'a':5, 'b':4})
#assert not _dict_subset({'a':5}, {'a':4, 'b':4})
#assert not _dict_subset({'a':5, 'c':1}, {'a':4, 'b':4})
#assert _dict_subset({'a':5, 'c':{}}, {'a':5, 'b':4, 'c':{'a':'abc'}})
#assert _dict_subset({'a':5, 'c':{'x':1}}, {'a':5, 'b':4, 'c':{'x':1,'a':'abc'}})
#assert not _dict_subset({'a':5, 'c':{'x':1}}, {'a':5, 'b':4, 'c':{'x':2,'a':'abc'}})

#assert not _dict_subset({'a':{'c':2}}, {'a':1, 'b':2})
#assert _dict_subset({'a':{'c':2}}, {'a':{'c':2}, 'b':2})
#assert _dict_subset({'a':{'c':2}}, {'a':{'c':2, 'd':3}, 'b':2})
