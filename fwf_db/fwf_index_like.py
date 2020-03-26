#!/usr/bin/env python
# encoding: utf-8

import abc


class FWFIndexLike(abc.ABC):

    def init_index_like(self, fwfview):

        self.fwfview = fwfview
        self.field = None


    @abc.abstractmethod
    def index(self, field, func=None):
        pass


    @abc.abstractmethod
    def __len__(self):
        pass


    @abc.abstractmethod
    def __iter__(self):     
        pass


    @abc.abstractmethod
    def fwf_subset(self, fwffile, key, fields):
        pass


    def __getitem__(self, key):
        '''Get lines matching where the Index matches key'''

        return self.fwf_subset(self.fwfview, key, self.fwfview.fields)


    def get(self, key):
        '''Get lines matching where the Index matches key'''

        return self.fwf_subset(self.fwfview, key, self.fwfview.fields)
