"use strict";

// module Errybody.UUID

exports.genUUIDImpl = function genUUIDImpl(version) {
    return require("uuid/" + version);
};
