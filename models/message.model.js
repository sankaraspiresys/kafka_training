const mongoose = require('mongoose');

const MessageSchema = mongoose.Schema({
    name: String,
    age: Number
}, {
    timestamps: true
});
mongoose.set('useFindAndModify', false);

module.exports = mongoose.model('Message', MessageSchema);