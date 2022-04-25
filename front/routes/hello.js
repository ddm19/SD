const express = require('express'),
router = express.Router(),
hello = require('../controllers/hello')


router.get('/',hello.db)

router.get('/ciudades',hello.temps)

module.exports = router