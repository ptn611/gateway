//! Defines types for:
//! - gateway request/responses
//! - wrappers for presenting velocity program types with less implementation detail
//!
use std::convert::TryInto;

use nanoid::nanoid;
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use velocity_rs::{
    constants::ProgramData,
    math::{
        constants::{BASE_PRECISION, PRICE_PRECISION, QUOTE_PRECISION},
        liquidation::{CollateralInfo, MarginRequirementInfo},
    },
    swift_order_subscriber::SignedOrderType,
    types::{
        self as sdk_types,
        accounts::{PerpMarket, SpotMarket},
        MarketPrecision, MarketType, ModifyOrderParams, OrderParams, OrderTriggerCondition,
        PositionDirection, PostOnlyParam, SdkError, SdkResult, SignedMsgOrderParamsDelegateMessage,
        SignedMsgOrderParamsMessage,
    },
    Pubkey,
};

use crate::websocket::AccountEvent;

/// decimal places in price values
pub const PRICE_DECIMALS: u32 = PRICE_PRECISION.ilog10();
pub const QUOTE_DECIMALS: u32 = QUOTE_PRECISION.ilog10();

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Order {
    #[serde(serialize_with = "order_type_ser", deserialize_with = "order_type_de")]
    order_type: sdk_types::OrderType,
    market_index: u16,
    #[serde(
        serialize_with = "ser_market_type",
        deserialize_with = "de_market_type"
    )]
    market_type: MarketType,
    amount: Decimal,
    filled: Decimal,
    price: Decimal,
    post_only: bool,
    reduce_only: bool,
    user_order_id: u8,
    order_id: u32,
    immediate_or_cancel: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    oracle_price_offset: Option<Decimal>,
}

impl Order {
    pub fn from_sdk_order(value: sdk_types::Order, base_decimals: u32) -> Self {
        // 0 = long
        // 1 = short
        let to_sign = 1_i64 - 2 * (value.direction as i64);

        Order {
            market_index: value.market_index,
            market_type: value.market_type,
            price: Decimal::new(value.price as i64, PRICE_DECIMALS),
            amount: Decimal::new(value.base_asset_amount as i64 * to_sign, base_decimals),
            filled: Decimal::new(value.base_asset_amount_filled as i64, base_decimals),
            immediate_or_cancel: value.immediate_or_cancel,
            reduce_only: value.reduce_only,
            order_type: value.order_type,
            order_id: value.order_id,
            post_only: value.post_only,
            user_order_id: value.user_order_id,
            oracle_price_offset: if value.oracle_price_offset == 0 {
                None
            } else {
                Some(Decimal::new(
                    value.oracle_price_offset as i64,
                    PRICE_DECIMALS,
                ))
            },
        }
    }
}

/// serde support for the program's `OrderTriggerCondition`, which derives only
/// anchor traits. Variant names match the enum so the JSON form is unchanged.
#[derive(Serialize, Deserialize)]
#[serde(remote = "OrderTriggerCondition")]
enum OrderTriggerConditionDef {
    Above,
    Below,
    TriggeredAbove,
    TriggeredBelow,
}

/// as [`OrderTriggerConditionDef`], for `Option` fields
mod opt_order_trigger_condition {
    use super::{OrderTriggerCondition, OrderTriggerConditionDef};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(
        value: &Option<OrderTriggerCondition>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        #[derive(Serialize)]
        struct Wrapper<'a>(#[serde(with = "OrderTriggerConditionDef")] &'a OrderTriggerCondition);
        value.as_ref().map(Wrapper).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<OrderTriggerCondition>, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wrapper(#[serde(with = "OrderTriggerConditionDef")] OrderTriggerCondition);
        Ok(Option::<Wrapper>::deserialize(deserializer)?.map(|w| w.0))
    }
}

fn order_type_ser<S>(order_type: &sdk_types::OrderType, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let s = match order_type {
        sdk_types::OrderType::Limit => "limit",
        sdk_types::OrderType::Market => "market",
        sdk_types::OrderType::TriggerLimit => "trigger_limit",
        sdk_types::OrderType::TriggerMarket => "trigger_market",
        sdk_types::OrderType::Oracle => "oracle",
    };
    serializer.serialize_str(s)
}

fn order_type_de<'de, D>(deserializer: D) -> Result<sdk_types::OrderType, D::Error>
where
    D: Deserializer<'de>,
{
    let order_type = Deserialize::deserialize(deserializer)?;
    match order_type {
        "limit" => Ok(sdk_types::OrderType::Limit),
        "market" => Ok(sdk_types::OrderType::Market),
        "trigger_limit" => Ok(sdk_types::OrderType::TriggerLimit),
        "trigger_market" => Ok(sdk_types::OrderType::TriggerMarket),
        "oracle" => Ok(sdk_types::OrderType::Oracle),
        _ => Err(serde::de::Error::custom("invalid order type")),
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SpotPosition {
    amount: Decimal,
    #[serde(rename = "type")]
    balance_type: String, // deposit or borrow
    market_index: u16,
}

impl SpotPosition {
    pub fn from_sdk_type(
        position: &sdk_types::SpotPosition,
        spot_market: &SpotMarket,
    ) -> SdkResult<Self> {
        let token_amount = position
            .get_token_amount(spot_market)
            .map_err(|err| SdkError::Anchor(Box::new(err.into())))?;
        Ok(Self {
            amount: Decimal::from_i128_with_scale(token_amount as i128, spot_market.decimals)
                .normalize(),
            market_index: position.market_index,
            balance_type: if position.balance_type == Default::default() {
                "deposit".into()
            } else {
                "borrow".into()
            },
        })
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PerpPosition {
    amount: Decimal,
    average_entry: Decimal,
    market_index: u16,
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    extended: Option<PerpPositionExtended>,
}

impl PerpPosition {
    pub fn set_extended_info(&mut self, ext: PerpPositionExtended) {
        self.extended = Some(ext);
    }
}

impl From<sdk_types::PerpPosition> for PerpPosition {
    fn from(value: sdk_types::PerpPosition) -> Self {
        let amount = Decimal::new(value.base_asset_amount, BASE_PRECISION.ilog10());
        let average_entry = Decimal::new(value.quote_entry_amount.abs(), PRICE_DECIMALS)
            .checked_div(amount.abs())
            .unwrap_or_default();

        Self {
            amount: amount.normalize(),
            market_index: value.market_index,
            average_entry: average_entry.normalize().round_dp(4),
            extended: None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SwapRequest {
    pub amount: Decimal,
    pub exact_in: bool,
    pub input_market: u16,
    pub output_market: u16,
    pub slippage_bps: u16,
    pub use_direct_routes: Option<bool>,
    pub exclude_dexes: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct TitanSwapRequest {
    pub amount: Decimal,
    pub exact_in: bool,
    pub input_market: u16,
    pub output_market: u16,
    pub slippage_bps: u16,
    pub use_direct_routes: Option<bool>,
    pub exclude_dexes: Option<String>,
    pub max_accounts: Option<usize>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PerpPositionExtended {
    pub liquidation_price: Decimal,
    pub unrealized_pnl: Decimal,
    pub unsettled_pnl: Decimal,
    pub oracle_price: Decimal,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ModifyOrdersRequest {
    pub orders: Vec<ModifyOrder>,
}

#[cfg_attr(test, derive(Default))]
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ModifyOrder {
    #[serde(flatten)]
    pub market: Market,
    amount: Option<Decimal>,
    price: Option<Decimal>,
    pub user_order_id: Option<u8>,
    pub order_id: Option<u32>,
    reduce_only: Option<bool>,
    oracle_price_offset: Option<Decimal>,
    max_ts: Option<i64>,
}

impl ModifyOrder {
    pub fn to_order_params(self, base_decimals: u32) -> ModifyOrderParams {
        let target_scale = 10_u32.pow(base_decimals);

        let (amount, direction) = if let Some(base_amount) = self.amount {
            let direction = if base_amount.is_sign_negative() {
                PositionDirection::Short
            } else {
                PositionDirection::Long
            };
            (
                Some(scale_decimal_to_u64(base_amount.abs(), target_scale)),
                Some(direction),
            )
        } else {
            (None, None)
        };

        let price = self
            .price
            .map(|p| scale_decimal_to_u64(p, PRICE_PRECISION as u32));

        let oracle_price_offset = self
            .oracle_price_offset
            .map(|p| scale_decimal_to_i64(p, PRICE_PRECISION as u32));

        ModifyOrderParams {
            base_asset_amount: amount,
            direction,
            price,
            reduce_only: self.reduce_only,
            oracle_price_offset,
            max_ts: self.max_ts,
            ..Default::default()
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PlaceOrdersRequest {
    pub orders: Vec<PlaceOrder>,
    #[serde(default)]
    pub place_order_type: PlaceOrderType,
}

#[cfg_attr(test, derive(Default))]
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PlaceOrder {
    #[serde(flatten)]
    pub market: Market,
    amount: Decimal,
    #[serde(default)]
    price: Decimal,
    #[serde(default)]
    trigger_price: Option<Decimal>,
    #[serde(default, with = "opt_order_trigger_condition")]
    trigger_condition: Option<OrderTriggerCondition>,
    /// 0 indicates it is not set (according to program)
    #[serde(default)]
    pub user_order_id: u8,
    #[serde(
        serialize_with = "order_type_ser",
        deserialize_with = "order_type_de",
        default
    )]
    order_type: sdk_types::OrderType,
    #[serde(default)]
    post_only: bool,
    #[serde(default)]
    reduce_only: bool,
    #[serde(default)]
    oracle_price_offset: Option<Decimal>,
    max_ts: Option<i64>,
    #[serde(default)]
    auction_duration: Option<u8>,
    #[serde(default)]
    auction_start_price: Option<i64>,
    #[serde(default)]
    auction_end_price: Option<i64>,
}

#[derive(Serialize, Debug)]
pub enum PlaceOrderType {
    Tx,
    SignedMsg,
}

impl Default for PlaceOrderType {
    fn default() -> Self {
        Self::Tx
    }
}

impl<'de> Deserialize<'de> for PlaceOrderType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: &str = Deserialize::deserialize(deserializer)?;
        match s {
            "tx" => Ok(PlaceOrderType::Tx),
            "swift" => Ok(PlaceOrderType::SignedMsg),
            _ => Err(serde::de::Error::custom(format!(
                "unknown place order type: {}",
                s
            ))),
        }
    }
}

pub fn ser_market_type<S>(x: &MarketType, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str(match x {
        MarketType::Perp => "perp",
        MarketType::Spot => "spot",
    })
}

pub fn de_market_type<'de, D>(deserializer: D) -> Result<MarketType, D::Error>
where
    D: Deserializer<'de>,
{
    let s: &str = Deserialize::deserialize(deserializer)?;
    match s {
        "perp" => Ok(MarketType::Perp),
        "spot" => Ok(MarketType::Spot),
        _ => Err(serde::de::Error::custom(format!(
            "unknown market type: {}",
            s
        ))),
    }
}

#[inline]
/// Convert decimal to unsigned fixed-point representation with `target` precision
pub fn scale_decimal_to_u64(x: Decimal, target: u32) -> u64 {
    ((x.mantissa().unsigned_abs() * target as u128) / 10_u128.pow(x.scale())) as u64
}

#[inline]
/// Convert decimal to unsigned fixed-point representation with `target` precision
fn scale_decimal_to_i64(x: Decimal, target: u32) -> i64 {
    ((x.mantissa() * target as i128) / 10_i128.pow(x.scale())) as i64
}

impl PlaceOrder {
    pub fn to_order_params(self, base_decimals: u32) -> OrderParams {
        let target_scale = 10_u32.pow(base_decimals);
        let base_amount = scale_decimal_to_u64(self.amount.abs(), target_scale);
        let price = if self.oracle_price_offset.is_none() {
            scale_decimal_to_u64(self.price, PRICE_PRECISION as u32)
        } else {
            0
        };

        let oracle_price_offset = self
            .oracle_price_offset
            .map(|x| scale_decimal_to_i64(x, PRICE_PRECISION as u32));

        OrderParams {
            market_index: self.market.market_index,
            market_type: self.market.market_type,
            order_type: self.order_type,
            base_asset_amount: base_amount,
            direction: if self.amount.is_sign_negative() {
                PositionDirection::Short
            } else {
                PositionDirection::Long
            },
            price,
            reduce_only: self.reduce_only,
            post_only: if self.post_only {
                PostOnlyParam::MustPostOnly // this will report the failure to the gateway caller
            } else {
                PostOnlyParam::None
            },
            user_order_id: self.user_order_id,
            oracle_price_offset,
            max_ts: self.max_ts,
            trigger_price: self
                .trigger_price
                .map(|v| scale_decimal_to_u64(v, PRICE_PRECISION as u32)),
            trigger_condition: self.trigger_condition.unwrap_or_default(),
            auction_duration: Some(self.auction_duration.unwrap_or(20)),
            auction_start_price: self.auction_start_price,
            auction_end_price: self.auction_end_price,
            ..Default::default()
        }
    }
}

/// How a swift order message is signed
///
/// The two forms are *not* interchangeable: they use different borsh prefixes and
/// the delegate form carries the taker's sub-account address in place of its id.
///
/// The form must match the signing key. The swift node derives `is_delegate_signer`
/// from the prefix (`SignedOrderType::is_delegated`), and the program then requires
/// the ed25519 signer to be `taker.authority` when that flag is false, or
/// `taker.delegate` when it is true. Signing with a delegate key while emitting the
/// authority form makes the program check the signature against the authority and
/// reject the order.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SwiftSigner {
    /// signed by the account authority
    Authority { sub_account_id: u16 },
    /// signed by an authorized delegate
    Delegate { taker_pubkey: Pubkey },
}

/// Build the hexified, borsh-encoded swift order message
///
/// Returns the order's `uuid` alongside the payload; it identifies the order to
/// the swift node and is not recoverable from the message otherwise.
pub fn to_signed_order_hex(
    order_params: OrderParams,
    slot: u64,
    signer: SwiftSigner,
) -> (String, Vec<u8>) {
    // nanoid's default alphabet is ASCII, so 8 chars is always 8 bytes
    let uuid_str = nanoid!(8);
    let uuid: [u8; 8] = uuid_str.as_bytes().try_into().expect("8 byte uuid");

    let signed_order_type = match signer {
        SwiftSigner::Authority { sub_account_id } => {
            SignedOrderType::authority(SignedMsgOrderParamsMessage {
                signed_msg_order_params: order_params,
                slot,
                uuid,
                sub_account_id,
                take_profit_order_params: None, // TODO: add take profit order params
                stop_loss_order_params: None,   // TODO: add stop loss order params
                max_margin_ratio: None,
                builder_idx: None,
                builder_fee_tenth_bps: None,
                isolated_position_deposit: None,
            })
        }
        SwiftSigner::Delegate { taker_pubkey } => {
            SignedOrderType::delegated(SignedMsgOrderParamsDelegateMessage {
                signed_msg_order_params: order_params,
                taker_pubkey,
                slot,
                uuid,
                take_profit_order_params: None, // TODO: add take profit order params
                stop_loss_order_params: None,   // TODO: add stop loss order params
                max_margin_ratio: None,
                builder_idx: None,
                builder_fee_tenth_bps: None,
                isolated_position_deposit: None,
            })
        }
    };

    let borsh_encoding = signed_order_type.to_borsh();
    let borsh_bytes = borsh_encoding.as_slice();
    let mut hex_bytes = vec![0; borsh_bytes.len() * 2]; // 2 hex bytes per msg byte
    faster_hex::hex_encode(borsh_bytes, &mut hex_bytes).expect("hexified");

    (uuid_str, hex_bytes)
}

#[cfg_attr(test, derive(Default))]
#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Market {
    /// The market index
    pub market_index: u16,
    #[serde(
        serialize_with = "ser_market_type",
        deserialize_with = "de_market_type"
    )]
    /// The market type (Spot or Perp)
    pub market_type: MarketType,
}

impl Market {
    pub fn new(market_index: u16, market_type: MarketType) -> Self {
        Self {
            market_index,
            market_type,
        }
    }
    pub fn spot(index: u16) -> Self {
        Self {
            market_index: index,
            market_type: MarketType::Spot,
        }
    }
    pub fn perp(index: u16) -> Self {
        Self {
            market_index: index,
            market_type: MarketType::Perp,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SetLeverageRequest {
    pub leverage: Decimal,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct GetPositionsRequest {
    #[serde(flatten)]
    pub market: Market,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct GetOrdersRequest {
    #[serde(flatten)]
    pub market: Market,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetOrdersResponse {
    pub orders: Vec<Order>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetPositionsResponse {
    pub spot: Vec<SpotPosition>,
    pub perp: Vec<PerpPosition>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketInfo {
    #[serde(rename = "marketIndex")]
    market_id: u16,
    symbol: String,
    price_step: Decimal,
    amount_step: Decimal,
    min_order_size: Decimal,
    #[serde(skip_serializing_if = "Option::is_none")]
    initial_margin_ratio: Option<Decimal>,
    #[serde(skip_serializing_if = "Option::is_none")]
    maintenance_margin_ratio: Option<Decimal>,
}

impl From<SpotMarket> for MarketInfo {
    fn from(value: SpotMarket) -> Self {
        Self {
            market_id: value.market_index,
            // `name` is raw on-chain bytes; `from_utf8_unchecked` here would be UB
            // for any market whose name is not valid utf8.
            symbol: String::from_utf8_lossy(&value.name).trim_end().to_string(),
            price_step: Decimal::new(value.price_tick() as i64, PRICE_PRECISION.ilog10())
                .normalize(),
            amount_step: Decimal::new(value.quantity_tick() as i64, value.decimals).normalize(),
            min_order_size: Decimal::new(value.min_order_size() as i64, value.decimals).normalize(),
            initial_margin_ratio: None,
            maintenance_margin_ratio: None,
        }
    }
}

impl From<PerpMarket> for MarketInfo {
    fn from(value: PerpMarket) -> Self {
        Self {
            market_id: value.market_index,
            // `name` is raw on-chain bytes; `from_utf8_unchecked` here would be UB
            // for any market whose name is not valid utf8.
            symbol: String::from_utf8_lossy(&value.name).trim_end().to_string(),
            price_step: Decimal::new(value.price_tick() as i64, PRICE_PRECISION.ilog10())
                .normalize(),
            amount_step: Decimal::new(value.quantity_tick() as i64, BASE_PRECISION.ilog10())
                .normalize(),
            min_order_size: Decimal::new(value.min_order_size() as i64, BASE_PRECISION.ilog10())
                .normalize(),
            initial_margin_ratio: Some(
                Decimal::new(value.margin_ratio_initial as i64, 4).normalize(),
            ),
            maintenance_margin_ratio: Some(
                Decimal::new(value.margin_ratio_maintenance as i64, 4).normalize(),
            ),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketInfoResponse {
    pub open_interest: u64,
    pub max_open_interest: u64,
}

#[derive(Serialize)]
pub struct AllMarketsResponse {
    pub spot: Vec<MarketInfo>,
    pub perp: Vec<MarketInfo>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(rename_all = "camelCase")]
pub struct CancelOrdersRequest {
    /// Market to cancel orders
    #[serde(flatten, default)]
    pub market: Option<Market>,
    /// order Ids to cancel
    #[serde(default)]
    pub ids: Option<Vec<u32>>,
    /// user assigned order Ids to cancel
    #[serde(default)]
    pub user_ids: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TxResponse {
    tx: String,
}

impl TxResponse {
    pub fn new(tx_signature: String) -> Self {
        Self { tx: tx_signature }
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct TxEventsResponse {
    events: Vec<AccountEvent>,
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl TxEventsResponse {
    pub fn new(events: Vec<AccountEvent>, success: bool, error: Option<String>) -> Self {
        Self {
            events,
            success,
            error,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SignedMsgResponse {
    pub results: Vec<SignedMsgOrderResult>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SignedMsgOrderResult {
    pub hash: String,
    /// the order's swift uuid, it is generated per order and not otherwise
    /// recoverable by the caller
    pub uuid: String,
    pub status: String,
    pub error: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase", untagged)]
pub enum PlaceOrderResponse {
    Tx(TxResponse),
    SignedMsg(SignedMsgResponse),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CancelAndPlaceRequest {
    pub cancel: CancelOrdersRequest,
    pub modify: ModifyOrdersRequest,
    pub place: PlaceOrdersRequest,
}

/// Return the number of decimal places for the market, `None` if the market index
/// is unknown.
///
/// The index is caller-supplied on the order paths, so an unknown one must be
/// reported rather than panic. Guessing a default would silently misscale the
/// order size.
#[inline]
pub(crate) fn get_market_decimals(program_data: &ProgramData, market: Market) -> Option<u32> {
    if let MarketType::Perp = market.market_type {
        Some(BASE_PRECISION.ilog10())
    } else {
        program_data
            .spot_market_config_by_index(market.market_index)
            .map(|spot_market| spot_market.decimals)
    }
}

#[derive(Serialize, Debug)]
pub struct SolBalanceResponse {
    pub balance: Decimal,
    pub pubkey: String,
}

#[derive(Serialize, Debug)]
pub struct AuthorityResponse {
    pub pubkey: String,
}

#[derive(Serialize, Debug)]
pub struct UserMarginResponse {
    pub initial: Decimal,
    pub maintenance: Decimal,
}

impl From<MarginRequirementInfo> for UserMarginResponse {
    fn from(value: MarginRequirementInfo) -> Self {
        Self {
            initial: Decimal::from_i128_with_scale(value.initial as i128, PRICE_DECIMALS)
                .normalize(),
            maintenance: Decimal::from_i128_with_scale(value.maintenance as i128, PRICE_DECIMALS)
                .normalize(),
        }
    }
}

#[derive(Serialize, Debug)]
pub struct UserLeverageResponse {
    pub leverage: Decimal,
}

impl From<u128> for UserLeverageResponse {
    fn from(value: u128) -> Self {
        Self {
            leverage: Decimal::from_i128_with_scale(value as i128, PRICE_DECIMALS).normalize(),
        }
    }
}

#[derive(Serialize, Debug)]
pub struct UserCollateralResponse {
    pub total: Decimal,
    pub free: Decimal,
}

impl From<CollateralInfo> for UserCollateralResponse {
    fn from(value: CollateralInfo) -> Self {
        Self {
            total: Decimal::from_i128_with_scale(value.total, QUOTE_DECIMALS).normalize(),
            free: Decimal::from_i128_with_scale(value.free, QUOTE_DECIMALS).normalize(),
        }
    }
}

#[derive(serde::Serialize, Clone, Debug, PartialEq)]
pub struct IncomingSignedMessage {
    pub taker_authority: String,
    pub signature: String,
    pub message: String,
    pub signing_authority: String,
    pub market_type: &'static str,
    pub market_index: u16,
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use velocity_rs::{
        math::constants::BASE_PRECISION,
        types::{MarketType, OrderTriggerCondition, OrderType, PositionDirection},
        Pubkey,
    };

    use super::{to_signed_order_hex, Decimal, PlaceOrder, SwiftSigner};
    use crate::types::{Market, ModifyOrder, Order};

    #[test]
    fn place_order_to_order() {
        let cases = [
            ("0.1234", 123_400_000_u64),
            ("123", 123_000_000_000),
            ("-123.456", 123_456_000_000),
            ("0.0034", 3_400_000),
            ("10.0023", 10_002_300_000),
            ("5.12345678911", 5_123_456_789),
        ];
        for (input, expected) in cases {
            let p = PlaceOrder {
                amount: Decimal::from_str(input).unwrap(),
                price: Decimal::from_str(input).unwrap(),
                market: Market::perp(0),
                ..Default::default()
            };
            let order_params = p.to_order_params(9);
            assert_eq!(order_params.base_asset_amount, expected);
            assert_eq!(
                order_params.price,
                expected / 1_000 // 1e9 - 1e6
            );
        }
    }

    #[test]
    fn place_order_to_order_spot() {
        let cases = [
            ("0.1234", 123_400u64, 6),
            ("123", 123_000_000_000, 9),
            ("1.23", 1_230_000_000, 9),
            ("-1.23", 1_230_000_000, 9),
            ("5.123456789", 512_345_678, 8), // truncates extra decimals
        ];
        for (input, expected, base_decimals) in cases {
            let p = PlaceOrder {
                amount: Decimal::from_str(input).unwrap(),
                price: Decimal::from_str(input).unwrap(),
                ..Default::default()
            };
            let is_short = p.amount.is_sign_negative();
            let order_params = p.to_order_params(base_decimals);
            assert_eq!(order_params.base_asset_amount, expected);
            if is_short {
                assert_eq!(order_params.direction, PositionDirection::Short);
            } else {
                assert_eq!(order_params.direction, PositionDirection::Long);
            }
        }
    }

    #[test]
    fn place_order_to_order_trigger_works() {
        let cases = [
            ("0.1234", Some(OrderTriggerCondition::Below), 123_400u64),
            ("123", Some(OrderTriggerCondition::Above), 123_000_000),
            (
                "1.23",
                Some(OrderTriggerCondition::TriggeredBelow),
                1_230_000,
            ),
            (
                "-1.23",
                Some(OrderTriggerCondition::TriggeredAbove),
                1_230_000,
            ),
            ("5.123456789", None, 5_123_456), // truncates extra decimals
        ];
        for (input_trigger_price, input_trigger_condition, expected) in cases {
            let p = PlaceOrder {
                order_type: OrderType::TriggerLimit,
                trigger_price: Decimal::from_str(input_trigger_price).ok(),
                trigger_condition: input_trigger_condition,
                market: Market::perp(0),
                amount: Decimal::from_str("1").unwrap(),
                ..Default::default()
            };
            let order_params = p.to_order_params(9);
            assert_eq!(order_params.trigger_price, Some(expected));
            assert_eq!(
                order_params.trigger_condition,
                input_trigger_condition.unwrap_or_default()
            );
        }
    }

    #[test]
    fn oracle_price_offset_works() {
        let p = PlaceOrder {
            price: Decimal::from_str("1.23").unwrap(),
            oracle_price_offset: Decimal::from_str("-0.5").ok(),
            order_type: OrderType::Limit,
            market: Market::perp(0),
            ..Default::default()
        };
        let order = p.to_order_params(6);
        assert_eq!(order.price, 0);
        assert_eq!(order.oracle_price_offset, Some(-500_000));

        let o = velocity_rs::types::Order {
            base_asset_amount: 1 * BASE_PRECISION as u64,
            price: 0,
            market_index: 0,
            market_type: MarketType::Perp.into(),
            oracle_price_offset: -500_000,
            ..Default::default()
        };
        let order = Order::from_sdk_order(o, BASE_PRECISION.ilog10());
        assert_eq!(order.price, Decimal::ZERO,);

        assert_eq!(order.oracle_price_offset, Decimal::from_str("-0.5").ok());
    }

    #[test]
    fn order_from_sdk_order() {
        let cases = [
            (1_230_400_000_u64, Decimal::from_str("1.2304").unwrap(), 9),
            (123_000_000_000, Decimal::from_str("123.0").unwrap(), 9),
            (5_123_456_789, Decimal::from_str("5.123456789").unwrap(), 9),
        ];
        for (input, expected, base_decimals) in cases {
            let o = velocity_rs::types::Order {
                base_asset_amount: input,
                price: input,
                market_type: MarketType::Perp.into(),
                ..Default::default()
            };
            let gateway_order = Order::from_sdk_order(o, base_decimals);
            assert_eq!(gateway_order.amount, expected);
        }
    }

    #[test]
    fn modify_order_to_order_params() {
        let m = ModifyOrder {
            amount: Decimal::from_str("-0.5").ok(),
            price: Decimal::from_str("11.1").ok(),
            oracle_price_offset: Decimal::from_str("0.1").ok(),
            ..Default::default()
        };
        let order_params = m.to_order_params(9);

        assert_eq!(order_params.direction, Some(PositionDirection::Short));
        assert_eq!(order_params.base_asset_amount, Some(500_000_000));
        assert_eq!(order_params.price, Some(11_100_000));
        assert_eq!(order_params.oracle_price_offset, Some(100_000));

        let m = ModifyOrder {
            amount: Decimal::from_str("12").ok(),
            price: Decimal::from_str("1.02").ok(),
            oracle_price_offset: Decimal::from_str("-2").ok(),
            ..Default::default()
        };
        let order_params = m.to_order_params(9);

        assert_eq!(order_params.direction, Some(PositionDirection::Long));
        assert_eq!(order_params.base_asset_amount, Some(12_000_000_000));
        assert_eq!(order_params.price, Some(1_020_000));
        assert_eq!(order_params.oracle_price_offset, Some(-2_000_000));
    }

    /// helper: hex payload -> raw borsh bytes
    fn decode(hex: &[u8]) -> Vec<u8> {
        let mut out = vec![0; hex.len() / 2];
        faster_hex::hex_decode(hex, &mut out).expect("valid hex");
        out
    }

    fn test_order_params() -> velocity_rs::types::OrderParams {
        PlaceOrder {
            amount: Decimal::from_str("1.5").unwrap(),
            price: Decimal::from_str("100").unwrap(),
            market: Market::perp(0),
            ..Default::default()
        }
        .to_order_params(9)
    }

    /// regression: the gateway always emitted the authority form, even when signing
    /// with a delegate key. the swift node reads `is_delegate_signer` off the prefix,
    /// so the program then required the ed25519 signer to be `taker.authority` while
    /// the payload had actually been signed by the delegate -- every delegated swift
    /// order was rejected on-chain.
    #[test]
    fn swift_delegate_uses_the_delegate_message_form() {
        use velocity_rs::swift_order_subscriber::{SWIFT_DELEGATE_MSG_PREFIX, SWIFT_MSG_PREFIX};

        let taker = Pubkey::new_unique();
        let (_, authority_hex) = to_signed_order_hex(
            test_order_params(),
            123,
            SwiftSigner::Authority { sub_account_id: 7 },
        );
        let (_, delegate_hex) = to_signed_order_hex(
            test_order_params(),
            123,
            SwiftSigner::Delegate {
                taker_pubkey: taker,
            },
        );

        let authority_msg = decode(&authority_hex);
        let delegate_msg = decode(&delegate_hex);

        assert_eq!(
            authority_msg[..8],
            SWIFT_MSG_PREFIX,
            "authority order must carry the authority prefix"
        );
        assert_eq!(
            delegate_msg[..8],
            SWIFT_DELEGATE_MSG_PREFIX,
            "delegate order must carry the delegate prefix"
        );

        // the delegate form identifies the taker by sub-account address; the
        // authority form has no pubkey at all, only a u16 id
        let taker_bytes = taker.to_bytes();
        assert!(
            delegate_msg
                .windows(taker_bytes.len())
                .any(|w| w == taker_bytes),
            "delegate message must embed the taker pubkey"
        );
        assert!(
            !authority_msg
                .windows(taker_bytes.len())
                .any(|w| w == taker_bytes),
            "authority message must not embed a taker pubkey"
        );
    }

    /// the uuid is generated internally and is not recoverable by the caller
    /// unless it is returned
    #[test]
    fn swift_returns_the_uuid_embedded_in_the_message() {
        let (uuid, hex) = to_signed_order_hex(
            test_order_params(),
            123,
            SwiftSigner::Authority { sub_account_id: 0 },
        );
        let msg = decode(&hex);

        assert_eq!(uuid.len(), 8, "uuid is 8 bytes on the wire");
        assert!(
            msg.windows(8).any(|w| w == uuid.as_bytes()),
            "returned uuid must be the one signed into the message"
        );
    }

    /// the program's `OrderTriggerCondition` has no serde derive, gateway
    /// provides its own. assert the JSON form matches the variant names
    #[test]
    fn trigger_condition_json_round_trips() {
        for (json, expected) in [
            ("Above", OrderTriggerCondition::Above),
            ("Below", OrderTriggerCondition::Below),
            ("TriggeredAbove", OrderTriggerCondition::TriggeredAbove),
            ("TriggeredBelow", OrderTriggerCondition::TriggeredBelow),
        ] {
            let body = format!(
                r#"{{"marketIndex":0,"marketType":"perp","amount":1.0,"triggerCondition":"{json}"}}"#
            );
            let order: PlaceOrder = serde_json::from_str(&body).expect("deserializes");
            assert_eq!(order.trigger_condition, Some(expected));

            let out = serde_json::to_value(&order).expect("serializes");
            assert_eq!(out["triggerCondition"], serde_json::json!(json));
        }
    }

    #[test]
    fn trigger_condition_defaults_to_none() {
        let body = r#"{"marketIndex":0,"marketType":"perp","amount":1.0}"#;
        let order: PlaceOrder = serde_json::from_str(body).expect("deserializes");
        assert!(order.trigger_condition.is_none());
    }
}
