// Copyright 2020 The Exonum Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Cryptocurrency database schema.

use exonum::{
    crypto::Hash,
    merkledb::{
        access::{Access, FromAccess, RawAccessMut},
        Group, ObjectHash, ProofListIndex, RawProofMapIndex,
    },
    runtime::CallerAddress as Address,
};
use exonum_derive::{FromAccess, RequireArtifact};

use crate::{wallet::Wallet, INITIAL_BALANCE};
use crate::{transactions::TxSendApprove};
use crate::{transactions::TxApprove};

/// Database schema for the cryptocurrency.
///
/// Note that the schema is crate-private, but it has a public part.
#[derive(Debug, FromAccess)]
pub(crate) struct SchemaImpl<T: Access> {
    /// Public part of the schema.
    #[from_access(flatten)]
    pub public: Schema<T>,
    /// History for specific wallets.
    pub wallet_history: Group<T, Address, ProofListIndex<T::Base, Hash>>,
}

/// Public part of the cryptocurrency schema.
#[derive(Debug, FromAccess, RequireArtifact)]
#[require_artifact(name = "exonum-cryptocurrency")]
pub struct Schema<T: Access> {
    /// Map of wallet keys to information about the corresponding account.
    pub wallets: RawProofMapIndex<T::Base, Address, Wallet>,
    /// Map of approval transactions hash to infromation about the corresponding approval transaction
    pub approval_transactions: RawProofMapIndex<T::Base, Hash, TxSendApprove>,
    /// Map of approved tx_send_approved transactions
    pub approved_transactions: RawProofMapIndex<T::Base, Hash, TxApprove>
}

impl<T: Access> SchemaImpl<T> {
    pub fn new(access: T) -> Self {
        Self::from_root(access).unwrap()
    }

    pub fn wallet(&self, address: Address) -> Option<Wallet> {
        self.public.wallets.get(&address)
    }
}

impl<T> SchemaImpl<T>
where
    T: Access,
    T::Base: RawAccessMut,
{
    /// Append new unapproved transaction record to db.
    /// 'wallet' - wallet of sender
    pub fn create_send_approve_transaction(&mut self, wallet: Wallet, amount: u64, to: Address, approver: Address, tx_hash: Hash) {
        // Update freezed balance & save the history
        self.change_wallet_balance(wallet, 0, amount as i64, tx_hash);

        // Save transaction in schema.approval_transactions
        let transaction = TxSendApprove::new(to, amount, approver);
        self.public.approval_transactions.put(&tx_hash, transaction);
    }

    /// Append new unapproved transaction record to db.
    /// 'wallet' - wallet of sender
    pub fn create_approve_transaction(&mut self, sender_wallet: Wallet, receiver_wallet: Wallet, amount: u64, tx_approve: TxApprove, tx_hash: Hash) {
        let neg_amount = (amount as i64) * -1;
        let pos_amount = amount as i64;
        
        // Update sender_wallet & save the history
        self.change_wallet_balance(sender_wallet, neg_amount, neg_amount, tx_hash);
        // Update receiver_wallet & save the history
        self.change_wallet_balance(receiver_wallet, pos_amount, 0, tx_hash);

        // Save transaction in schema.approved_transactions
        self.public.approved_transactions.put(&tx_hash, tx_approve.clone());
    }

    pub fn change_wallet_balance(&mut self, wallet: Wallet, balance_change: i64, freezed_balance_change: i64, transaction: Hash) {
        // Save transaction in wallet's history
        let mut history = self.wallet_history.get(&wallet.owner);
        history.push(transaction);
        let history_hash = history.object_hash();

        let wallet_freezed_balance = wallet.freezed_balance;
        let wallet_balance = wallet.balance;

        let wallet = wallet.set_balance(((wallet_balance as i64) + balance_change) as u64, &history_hash);
        let wallet = wallet.set_freezed_balance(((wallet_freezed_balance as i64) + freezed_balance_change) as u64, &history_hash);

        // storing in wallets-db
        let wallet_key = wallet.owner;
        self.public.wallets.put(&wallet_key, wallet);
    }

    /// Increases balance of the wallet and append new record to its history.
    pub fn increase_wallet_balance(&mut self, wallet: Wallet, amount: u64, transaction: Hash) {
        let mut history = self.wallet_history.get(&wallet.owner);
        history.push(transaction);
        let history_hash = history.object_hash();
        let balance = wallet.balance;
        let wallet = wallet.set_balance(balance + amount, &history_hash);
        let wallet_key = wallet.owner;
        self.public.wallets.put(&wallet_key, wallet);
    }

    /// Decreases balance of the wallet and append new record to its history.
    pub fn decrease_wallet_balance(&mut self, wallet: Wallet, amount: u64, transaction: Hash) {
        let mut history = self.wallet_history.get(&wallet.owner);
        history.push(transaction);
        let history_hash = history.object_hash();
        let balance = wallet.balance;
        let wallet = wallet.set_balance(balance - amount, &history_hash);
        let wallet_key = wallet.owner;
        self.public.wallets.put(&wallet_key, wallet);
    }

    /// Creates a new wallet and append first record to its history.
    pub fn create_wallet(&mut self, key: Address, name: &str, transaction: Hash) {
        let mut history = self.wallet_history.get(&key);
        history.push(transaction);
        let history_hash = history.object_hash();
        let wallet = Wallet::new(key, name, INITIAL_BALANCE, 0, history.len(), &history_hash);
        self.public.wallets.put(&key, wallet);
    }
}
