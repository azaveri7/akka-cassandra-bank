package com.paathshala.actors

import akka.actor.typed.{ActorRef, Behavior}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}

// a single bank account

// we are going to follow event sourcing model.
class PersistentBankAccount {

  /*
  - fault tolerance
  - auditing
   */

  // commands - messages
  sealed trait Command

  case class CreateBankAccount(user: String, currency: String,
                              initialBalance: Double,
                              replyTo: ActorRef[Response]) extends Command
  case class UpdateBalance(id: String, currency: String, amount: Double,
                           replyTo: ActorRef[Response]) extends Command
  case class GetBankAccount(id: String, replyTo: ActorRef[Response])
                           extends Command

  // events - to persist to Cassandra
  trait Event
  case class BankAccountCreated(bankAccount: BankAccount) extends Event
  case class BalanceUpdated(amount: Double) extends Event

  // state
  case class BankAccount(id: String, user: String, currency: String
                         , balance: Double)
  // responses
  sealed trait Response
  case class BankAccountCreatedResponse(id: String) extends Response
  case class BankAccountBalanceUpdatedResponse(maybeAccount: Option[BankAccount])
                         extends Response
  case class GetBankAccountResponse(maybeAccount: Option[BankAccount])
                         extends Response

  // command handler - message handler => persist an event
  // event handler => update state
  // state

  val commandHandler: (BankAccount, Command) => Effect[Event, BankAccount] =
    (state, command) =>
      command match {
        case CreateBankAccount(user, currency, initialBalance, replyTo) =>
          val id = state.id
          /*
          - bank creates me
          - bank send me CreateBankAccount
          - I persist BankAccountCreated
          - I update my state
           - reply back to bank with the BankAccountCreatedResponse
           - (the bank surfaces the response to the HTTP server)
           */
          Effect
            .persist(BankAccountCreated(
              BankAccount(id, user, currency, initialBalance))
            ) // persisted into Cassandra
            .thenReply(replyTo)(_ => BankAccountCreatedResponse(id))

        case UpdateBalance(_, _, amount, bank) =>
          val newBalance = state.balance + amount
          // check here for withdrawl
          if(newBalance < 0) // illegal
            Effect.reply(bank)(BankAccountBalanceUpdatedResponse(None))
          else
            Effect
              .persist(BalanceUpdated(amount))
              .thenReply(bank)(
                newState => BankAccountBalanceUpdatedResponse(Some(newState)))

        case GetBankAccount(_, bank) =>
          Effect.reply(bank)(GetBankAccountResponse(Some(state)))
      }

  val eventHandler: (BankAccount, Event) => BankAccount = (state, event) =>
    event match {
      case BankAccountCreated(bankAccount) =>
        bankAccount
      case BalanceUpdated(amount) =>
        state.copy(balance = state.balance + amount)
    }

  def apply(id: String): Behavior[Command] =
    EventSourcedBehavior[Command, Event, BankAccount](
      persistenceId = PersistenceId.ofUniqueId(id),
      emptyState = BankAccount(id, "", "", 0.0),//unused
      commandHandler = commandHandler,
      eventHandler = eventHandler
    )

}
