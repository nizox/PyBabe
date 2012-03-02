
from pybabe import Babe

import unittest

class TestBasicFunction(unittest.TestCase):

    def test_pull_push(self):
        babe = Babe()
        a = babe.pull('tests/test.csv', name='Test').typedetect()
        a = a.map('foo', lambda x : -x).multimap({'bar' : lambda x : x + 1, 'f' : lambda f : f / 2 }).sort('foo')
        a = a.groupkey('foo', int.__add__, 0, keepOriginal=True)
        a.push('tests/test2.csv')
        
    def test_keynormalize(self):
        babe = Babe()
        self.assertEqual('Payant_Gratuit', babe.keynormalize('Payant/Gratuit'))
    
class TestExcel(unittest.TestCase):
    
    def test_excel_read_write(self):
        babe = Babe()
        b = babe.pull('tests/test.xlsx', name='Test2').typedetect()
        b = b.map('Foo', lambda x : -x)
        b.push('tests/test2.xlsx')

if __name__ == "__main__":
    unittest.main()